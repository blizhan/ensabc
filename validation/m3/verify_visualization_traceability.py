#!/usr/bin/env python3
"""Six-pass traceability verifier and 10-case tamper self-check for M3 visualization (T038).

Implements `specs/004-m3-scenario-evolution/contracts/visualization.md` §3:
  Pass 1: Strict offline CSP header & zero external URL / CDN / remote font/script scan.
  Pass 2: Strict `xml.etree.ElementTree` SVG parse, adjacent-batch endpoint check,
          0 cross-gap edges, and 0 untagged geometric primitives (orphan shapes == 0).
  Pass 3: SVG DOM <-> embedded canonical JSON bijection & attribute equality +
          `#ensabc-inspector-drawer` & `#ensabc-provenance-ledger` binding check.
  Pass 4: HTML <-> DuckDB 6-table (+ `replay_batches` & M2 tables when present)
          100% field-by-field ground-truth audit (`rtol/atol <= 1e-15` on floats).
  Pass 5: Visible `#ensabc-disclaimer-banner` (3 mandatory non-physical / non-calibrated
          clauses) & prohibited affirmative overclaim scan.
  Pass 6: `--self-check` executing all 10 negative tamper mutations + incomplete
          provenance render rejection, asserting every tamper exits `1`.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import datetime as dt
from html.parser import HTMLParser
import io
import json
import math
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Callable
import xml.etree.ElementTree as ET

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import ensure_no_symlink_inside
from validation.m3.render_evolution_html import (
    DISCLAIMER_VERSION,
    NON_PHYSICAL_TOKEN,
    SCHEMA_VERSION,
    MissingProvenanceError,
    _fetch_evolution_history_packed,
    atomic_write,
    atomic_write_json,
    build_view_manifest,
    canonical_json_str,
    is_finite_number,
    main as render_cli_main,
    normalize_floats,
    parse_duckdb_json,
    render_from_db,
    run_isolated_duckdb,
    safe_embedded_json_str,
    sha256_bytes,
    sha256_file,
)

AUDIT_SCHEMA_VERSION = "m3.visualization-audit.v1"
SELF_CHECK_SCHEMA_VERSION = "m3.tamper-self-check.v1"
FLOAT_TOL = 1e-15

GEOMETRIC_PRIMITIVE_TAGS = frozenset(
    {"path", "line", "polyline", "polygon", "rect", "circle", "ellipse", "text", "tspan", "use"}
)

FORBIDDEN_EXTERNAL_TAGS = frozenset(
    {
        "link",
        "base",
        "iframe",
        "frame",
        "frameset",
        "object",
        "embed",
        "applet",
        "audio",
        "video",
        "source",
        "img",
        "image",
        "feimage",
        "foreignobject",
    }
)

EXPECTED_CSP_DIRECTIVES: dict[str, list[str]] = {
    "default-src": ["'none'"],
    "style-src": ["'unsafe-inline'"],
    "script-src": ["'unsafe-inline'"],
}

EXPECTED_LEGEND_TEXTS: tuple[str, ...] = (
    "Primary Backbone",
    "Secondary Split / Merge",
    "High Uncertainty / Ambiguous Tie",
    "Gap Chain-Break Barrier",
)

# Affirmative overclaim patterns forbidden by FR-008 / Edge Case 11 / Pass 5.
# Carefully anchored so mandatory negations ("不代表...", "does NOT represent...", "未经概率校准",
# "not a calibrated probability", "non_physical_member_migration") never false-positive.
PROHIBITED_CLAIM_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "affirmative_physical_member_migration_zh",
        re.compile(
            r"(?<!不)(?<!非)(?:证实|证明|代表|表明|意味着)[^。；\n]{0,20}"
            r"(?:物理(?:集合)?成员[^。；\n]{0,12}迁移|成员[^。；\n]{0,12}物理[^。；\n]{0,8}迁移)"
        ),
    ),
    (
        "affirmative_physical_member_migration_en",
        re.compile(
            r"(?<!not\s)(?<!not\srepresent\s)(?<!non-)(?:represents?|proves?|confirms?|tracks?)\s+"
            r"(?:actual\s+|real\s+)?physical\s+(?:ensemble\s+)?member\s+migration",
            re.IGNORECASE,
        ),
    ),
    (
        "affirmative_physical_cyclogenesis_zh",
        re.compile(
            r"(?<!不)(?<!非)(?:证实|证明|代表|表明)[^。；\n]{0,18}"
            r"(?:(?:天气系统|气旋)[^。；\n]{0,10}物理(?:生成|新生|消亡|灭亡)|物理(?:气旋生成|天气系统新生|天气系统消亡))"
        ),
    ),
    (
        "affirmative_physical_cyclogenesis_en",
        re.compile(
            r"(?<!not\s)(?:represents?|proves?|confirms?)\s+physical\s+(?:cyclogenesis|lysis|weather\s+system\s+birth)",
            re.IGNORECASE,
        ),
    ),
    (
        "affirmative_calibrated_probability_zh",
        re.compile(r"(?<!未经)(?<!非)(?<!未)(?:已完成|经过|属于|提供)概率校准|已校准(?:预报)?概率"),
    ),
    (
        "affirmative_calibrated_probability_en",
        re.compile(
            r"(?<!not\sa\s)(?<!un)(?<!non-)calibrated\s+(?:forecast\s+)?probabilit(?:y|ies)",
            re.IGNORECASE,
        ),
    ),
    (
        "affirmative_trading_or_skill_claim_zh",
        re.compile(r"(?:保证|证实|具备|直接用于)(?:可盈利的?)?(?:交易收益|交易信号|交易阿尔法|预报修正技巧提升)"),
    ),
    (
        "affirmative_trading_or_skill_claim_en",
        re.compile(
            r"(?:proven|guaranteed|profitable|actionable)\s+(?:trading\s+(?:alpha|signal|value|strategy)|forecast\s+skill\s+gain)",
            re.IGNORECASE,
        ),
    ),
)

NODE_ID_RE = re.compile(r"^b(\d+):c(\d+)$")
EDGE_ID_RE = re.compile(r"^b(\d+):c(\d+)->b(\d+):c(\d+)$")


class _DOMInspectorParser(HTMLParser):
    """Pure-stdlib HTML parser collecting tags, attributes, ancestors, styles, scripts, and banner text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.elements: list[dict[str, Any]] = []
        self.stack: list[dict[str, Any]] = []
        self.styles: list[str] = []
        self.scripts: list[tuple[dict[str, str], str]] = []
        self.visible_text_chunks: list[str] = []
        self.banner_info: dict[str, Any] | None = None
        self.banner_count = 0
        self.inspector_drawer_found = False
        self.provenance_ledger_found = False
        self.ledger_kv: dict[str, str] = {}
        self.ledger_duplicate_keys: list[str] = []
        self._in_head = False
        self._current_buf: list[str] | None = None
        self._current_buf_kind: str | None = None
        self._current_script_attrs: dict[str, str] = {}
        self._banner_depth: int | None = None
        self._banner_text: list[str] = []
        self._ledger_depth: int | None = None
        self._in_ledger_tr = False
        self._ledger_tr_ths: list[str] = []
        self._ledger_tr_tds: list[str] = []
        self._ledger_cell_kind: str | None = None
        self._ledger_cell_buf: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        attr_map = {k.lower(): ("" if v is None else v) for k, v in attrs}
        ancestors = [(item["tag"], dict(item["attrs"])) for item in self.stack]
        rec = {
            "tag": t,
            "attrs": attr_map,
            "in_head": self._in_head or (t == "head"),
            "ancestors": ancestors,
        }
        self.elements.append(rec)

        if t == "head":
            self._in_head = True
        if attr_map.get("id") == "ensabc-inspector-drawer" and t == "aside":
            self.inspector_drawer_found = True
        if attr_map.get("id") == "ensabc-provenance-ledger" and t == "details":
            self.provenance_ledger_found = True

        void_tags = {"meta", "link", "img", "br", "hr", "input", "source", "base", "area", "col", "embed", "param", "track", "wbr"}
        if t not in void_tags:
            self.stack.append(rec)

        if attr_map.get("id") == "ensabc-provenance-ledger" and t == "details":
            self._ledger_depth = len(self.stack)

        if self._ledger_depth is not None:
            if t == "tr":
                self._in_ledger_tr = True
                self._ledger_tr_ths = []
                self._ledger_tr_tds = []
            elif self._in_ledger_tr and t in ("th", "td"):
                self._ledger_cell_kind = t
                self._ledger_cell_buf = []

        if attr_map.get("id") == "ensabc-disclaimer-banner":
            self.banner_count += 1
            if self.banner_info is None:
                self.banner_info = {
                    "tag": t,
                    "attrs": attr_map,
                    "ancestors": ancestors,
                    "text": "",
                }
                if t not in void_tags:
                    self._banner_depth = len(self.stack)

        if t == "style":
            self._current_buf = []
            self._current_buf_kind = "style"
        elif t == "script":
            self._current_buf = []
            self._current_buf_kind = "script"
            self._current_script_attrs = attr_map

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t == "head":
            self._in_head = False
        if t == "style" and self._current_buf_kind == "style" and self._current_buf is not None:
            self.styles.append("".join(self._current_buf))
            self._current_buf = None
            self._current_buf_kind = None
        elif t == "script" and self._current_buf_kind == "script" and self._current_buf is not None:
            self.scripts.append((self._current_script_attrs, "".join(self._current_buf)))
            self._current_buf = None
            self._current_buf_kind = None

        if self._ledger_depth is not None:
            if self._in_ledger_tr and t in ("th", "td") and self._ledger_cell_kind == t and self._ledger_cell_buf is not None:
                cell_txt = " ".join("".join(self._ledger_cell_buf).split())
                if t == "th":
                    self._ledger_tr_ths.append(cell_txt)
                else:
                    self._ledger_tr_tds.append(cell_txt)
                self._ledger_cell_kind = None
                self._ledger_cell_buf = None
            elif t == "tr" and self._in_ledger_tr:
                if len(self._ledger_tr_ths) == 1 and len(self._ledger_tr_tds) == 1:
                    k_cell = self._ledger_tr_ths[0]
                    v_cell = self._ledger_tr_tds[0]
                    if k_cell in self.ledger_kv:
                        self.ledger_duplicate_keys.append(k_cell)
                    self.ledger_kv[k_cell] = v_cell
                self._in_ledger_tr = False

        if self._ledger_depth is not None and len(self.stack) == self._ledger_depth and self.stack and self.stack[-1]["tag"] == t:
            self._ledger_depth = None
            self._in_ledger_tr = False
            self._ledger_cell_kind = None
            self._ledger_cell_buf = None

        if self._banner_depth is not None and len(self.stack) == self._banner_depth and self.stack and self.stack[-1]["tag"] == t:
            if self.banner_info is not None:
                self.banner_info["text"] = " ".join("".join(self._banner_text).split())
            self._banner_depth = None

        for idx in range(len(self.stack) - 1, -1, -1):
            if self.stack[idx]["tag"] == t:
                del self.stack[idx:]
                break

    def handle_data(self, data: str) -> None:
        if self._current_buf is not None:
            self._current_buf.append(data)
            return
        if data.strip():
            self.visible_text_chunks.append(data)
        if self._banner_depth is not None:
            self._banner_text.append(data)
        if self._ledger_cell_buf is not None:
            self._ledger_cell_buf.append(data)


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _class_tokens(elem: Any) -> set[str]:
    cls = elem.attrib.get("class", "") if hasattr(elem, "attrib") else elem.get("class", "")
    return {tok for tok in cls.split() if tok}


def _floats_equal(a: float, b: float) -> bool:
    if not (is_finite_number(a) and is_finite_number(b)):
        return False
    return math.isclose(float(a), float(b), rel_tol=FLOAT_TOL, abs_tol=FLOAT_TOL)


def _deep_compare(
    actual: Any,
    expected: Any,
    path: str,
    findings: list[str],
    *,
    max_findings: int = 30,
) -> None:
    if len(findings) >= max_findings:
        return
    if expected is None or actual is None:
        if actual is not expected:
            findings.append(f"{path}: expected {expected!r}, got {actual!r}")
        return
    if isinstance(expected, bool) or isinstance(actual, bool):
        if type(actual) is not bool or actual != expected:
            findings.append(f"{path}: expected bool {expected!r}, got {actual!r}")
        return
    if isinstance(expected, int) and not isinstance(expected, bool):
        if type(actual) is not int or actual != expected:
            findings.append(f"{path}: expected int {expected!r}, got {actual!r}")
        return
    if isinstance(expected, float):
        if not _floats_equal(actual, expected):
            findings.append(f"{path}: float mismatch expected={expected!r}, got={actual!r}")
        return
    if isinstance(expected, str):
        if not isinstance(actual, str) or actual != expected:
            findings.append(f"{path}: string mismatch expected={expected!r}, got={actual!r}")
        return
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            findings.append(
                f"{path}: list length mismatch expected={len(expected)}, got={len(actual) if isinstance(actual, list) else type(actual).__name__}"
            )
            return
        for i, (av, ev) in enumerate(zip(actual, expected)):
            _deep_compare(av, ev, f"{path}[{i}]", findings, max_findings=max_findings)
        return
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            findings.append(f"{path}: expected dict, got {type(actual).__name__}")
            return
        exp_keys = set(expected.keys())
        act_keys = set(actual.keys())
        for mk in sorted(exp_keys - act_keys):
            findings.append(f"{path}: missing required key {mk!r}")
        for ek in sorted(act_keys - exp_keys):
            findings.append(f"{path}: unexpected extra key {ek!r}")
        for k in sorted(exp_keys & act_keys):
            _deep_compare(actual[k], expected[k], f"{path}.{k}", findings, max_findings=max_findings)
        return
    if actual != expected:
        findings.append(f"{path}: mismatch expected={expected!r}, got={actual!r}")


def _strip_css_comments(css: str) -> str:
    return re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)


def _css_has_url_or_import(css: str) -> bool:
    cleaned = _strip_css_comments(css)
    return re.search(r"@\s*import\b|\burl\s*\(", cleaned, flags=re.IGNORECASE) is not None


def _pass_1_offline_csp(html_text: str, dom: _DOMInspectorParser) -> dict[str, Any]:
    findings: list[str] = []
    csp_metas = [
        el
        for el in dom.elements
        if el["tag"] == "meta"
        and el["attrs"].get("http-equiv", "").lower() == "content-security-policy"
    ]
    if len(csp_metas) != 1:
        findings.append(f"Expected exactly 1 Content-Security-Policy <meta>, found {len(csp_metas)}")
    else:
        meta = csp_metas[0]
        if not meta["in_head"]:
            findings.append("CSP <meta> must reside inside <head>")
        content = meta["attrs"].get("content", "")
        directives: dict[str, list[str]] = {}
        for part in content.split(";"):
            part = part.strip()
            if not part:
                continue
            tokens = part.split()
            d_name = tokens[0].lower()
            d_vals = [tok.lower() for tok in tokens[1:]]
            if d_name in directives:
                findings.append(f"Duplicate CSP directive {d_name!r} forbidden")
            directives[d_name] = d_vals

        if directives != EXPECTED_CSP_DIRECTIVES:
            extra_dirs = sorted(set(directives.keys()) - set(EXPECTED_CSP_DIRECTIVES.keys()))
            missing_dirs = sorted(set(EXPECTED_CSP_DIRECTIVES.keys()) - set(directives.keys()))
            if extra_dirs:
                findings.append(f"Unexpected extra CSP directives forbidden: {extra_dirs}")
            if missing_dirs:
                findings.append(f"Missing required CSP directives: {missing_dirs}")
            for d_key, exp_tokens in EXPECTED_CSP_DIRECTIVES.items():
                if d_key in directives and directives[d_key] != exp_tokens:
                    findings.append(
                        f"CSP directive {d_key} must be {exp_tokens}, got {directives[d_key]}"
                    )

    url_scheme_re = re.compile(
        r"(?:https?:|wss?:|ftp:|file:|blob:|data:|javascript:|vbscript:)",
        re.IGNORECASE,
    )
    proto_rel_re = re.compile(r"(?:^|[\s\"'(,=])//[a-zA-Z0-9._-]+")
    safe_frag_url_re = re.compile(r"^url\(\s*#[A-Za-z0-9_.:-]+\s*\)$", re.IGNORECASE)

    for el in dom.elements:
        tag = el["tag"]
        attrs = el["attrs"]
        if tag in FORBIDDEN_EXTERNAL_TAGS:
            findings.append(f"Forbidden external/subresource-capable tag <{tag}> present")

        for attr_k, attr_v in attrs.items():
            if tag == "svg" and attr_k == "xmlns" and attr_v == "http://www.w3.org/2000/svg":
                continue
            if attr_k in ("src", "srcset", "xlink:href") or attr_k.endswith(":href"):
                findings.append(f"Forbidden external attribute {attr_k!r} on <{tag}>: {attr_v!r}")
                continue
            if attr_k in ("href", "action", "formaction", "poster", "data"):
                stripped_v = attr_v.strip()
                if not stripped_v.startswith("#") or len(stripped_v) < 2:
                    findings.append(
                        f"Non-fragment URI attribute {attr_k}={attr_v!r} on <{tag}> forbidden (must start with '#')"
                    )
                continue
            if attr_k == "style":
                if _css_has_url_or_import(attr_v):
                    findings.append(f"CSS url() or @import forbidden in inline style on <{tag}>")
                if url_scheme_re.search(attr_v) or proto_rel_re.search(attr_v):
                    findings.append(f"External URL in inline style on <{tag}>")
                continue
            if "url(" in attr_v.lower() and not safe_frag_url_re.fullmatch(attr_v.strip()):
                findings.append(f"Non-fragment url() in <{tag} {attr_k}={attr_v!r}> forbidden")
            if url_scheme_re.search(attr_v):
                findings.append(f"External/forbidden URI scheme in <{tag} {attr_k}={attr_v!r}>")
            if proto_rel_re.search(attr_v):
                findings.append(f"Protocol-relative external URL in <{tag} {attr_k}={attr_v!r}>")

    for idx, css in enumerate(dom.styles):
        cleaned_css = _strip_css_comments(css)
        if _css_has_url_or_import(css):
            findings.append(f"<style>[{idx}] contains forbidden CSS url() or @import")
        if url_scheme_re.search(cleaned_css) or proto_rel_re.search(cleaned_css):
            findings.append(f"<style>[{idx}] contains external URL reference")

    for idx, (s_attrs, js) in enumerate(dom.scripts):
        if s_attrs.get("type", "").lower() == "application/json":
            continue
        if re.search(r"(?:https?:|wss?:|ftp:)//", js, re.IGNORECASE):
            findings.append(f"<script>[{idx}] (id={s_attrs.get('id', '')!r}) contains external URL scheme")

    return {"status": "pass" if not findings else "fail", "findings": findings}


def _extract_svg_xml(html_text: str) -> str:
    all_svg_opens = re.findall(r"<svg\b", html_text, flags=re.IGNORECASE)
    if len(all_svg_opens) != 1:
        raise ValueError(f"Expected exactly 1 <svg> element in document, found {len(all_svg_opens)}")
    matches = list(
        re.finditer(
            r"<svg\b[^>]*\bid\s*=\s*(['\"])ensabc-evolution-svg\1[^>]*>.*?</svg>",
            html_text,
            flags=re.DOTALL | re.IGNORECASE,
        )
    )
    if len(matches) != 1:
        raise ValueError(f"Expected exactly 1 <svg id='ensabc-evolution-svg'>, found {len(matches)}")
    return matches[0].group(0)


def _pass_2_svg_xml_and_zero_orphans(
    html_text: str,
    dom: _DOMInspectorParser | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    findings: list[str] = []
    extracted: dict[str, Any] = {
        "batches": {},
        "gap_markers": {},
        "nodes": {},
        "edges": {},
        "orphan_primitives": [],
    }
    if dom is not None:
        svg_dom_els = [el for el in dom.elements if el["tag"] == "svg"]
        if len(svg_dom_els) != 1:
            findings.append(f"Expected svg_count == 1 in HTML DOM, found {len(svg_dom_els)}")
        elif svg_dom_els[0]["attrs"].get("id") != "ensabc-evolution-svg":
            findings.append(
                f"Single <svg> must have id='ensabc-evolution-svg', got {svg_dom_els[0]['attrs'].get('id')!r}"
            )

    try:
        svg_xml = _extract_svg_xml(html_text)
    except ValueError as exc:
        return {"status": "fail", "findings": [*findings, str(exc)]}, extracted

    try:
        svg_root = ET.fromstring(svg_xml)
    except ET.ParseError as exc:
        return {"status": "fail", "findings": [*findings, f"SVG XML parse failure: {exc}"]}, extracted

    if _strip_ns(svg_root.tag) != "svg":
        findings.append(f"Root SVG tag mismatch: {svg_root.tag}")
    if svg_root.tag != "{http://www.w3.org/2000/svg}svg":
        findings.append("SVG root missing xmlns='http://www.w3.org/2000/svg'")

    # Validate exact structural chrome groups by fixed IDs (never blindly trusting data-decorative="true")
    whitelisted_chrome_primitives: set[ET.Element] = set()
    seen_chrome_ids: set[str] = set()

    for top_child in list(svg_root):
        tc_tag = _strip_ns(top_child.tag)
        tc_id = top_child.attrib.get("id", "")
        if tc_id in ("ensabc-svg-bg", "ensabc-legend", "ensabc-lane-guides"):
            if tc_id in seen_chrome_ids:
                findings.append(f"Duplicate structural SVG chrome id={tc_id!r}")
                continue
            seen_chrome_ids.add(tc_id)
            if tc_id == "ensabc-svg-bg":
                if tc_tag == "rect" and len(list(top_child)) == 0:
                    whitelisted_chrome_primitives.add(top_child)
                else:
                    findings.append("#ensabc-svg-bg must be an empty <rect>")
            elif tc_id == "ensabc-legend":
                ch_list = list(top_child)
                rects = [c for c in ch_list if _strip_ns(c.tag) == "rect" and len(list(c)) == 0]
                lines_c = [c for c in ch_list if _strip_ns(c.tag) == "line" and len(list(c)) == 0]
                texts = [c for c in ch_list if _strip_ns(c.tag) == "text" and len(list(c)) == 0]
                text_vals = tuple((c.text or "").strip() for c in texts)
                if (
                    tc_tag == "g"
                    and len(ch_list) == 10
                    and len(rects) == 2
                    and len(lines_c) == 4
                    and len(texts) == 4
                    and text_vals == EXPECTED_LEGEND_TEXTS
                ):
                    whitelisted_chrome_primitives.update(ch_list)
                else:
                    findings.append("#ensabc-legend contains unexpected or tampered primitives")
            elif tc_id == "ensabc-lane-guides":
                ch_list = list(top_child)
                if tc_tag == "g" and all(_strip_ns(c.tag) == "line" and len(list(c)) == 0 for c in ch_list):
                    whitelisted_chrome_primitives.update(ch_list)
                else:
                    findings.append("#ensabc-lane-guides may contain only lane guide <line> elements")

    seen_batch_headers: set[int] = set()

    def walk(elem: ET.Element, ancestors: list[ET.Element]) -> None:
        local = _strip_ns(elem.tag)
        local_low = local.lower()
        classes = _class_tokens(elem)
        chain = ancestors + [elem]

        if ancestors and local_low == "svg":
            findings.append("Nested <svg> island forbidden inside #ensabc-evolution-svg")
        if local_low in FORBIDDEN_EXTERNAL_TAGS:
            findings.append(f"Forbidden SVG element <{local}> present")
        for ak, av in elem.attrib.items():
            ak_local = _strip_ns(ak).lower()
            if ak_local in ("src", "srcset") or (ak != "xmlns" and ak_local == "href" and "}" in ak):
                findings.append(f"Forbidden external SVG attribute {ak}={av!r} on <{local}>")
            elif ak_local == "href" and not av.strip().startswith("#"):
                findings.append(f"Non-fragment href={av!r} on <{local}> forbidden")

        if "evo-batch-col" in classes:
            ord_raw = elem.attrib.get("data-batch-ordinal")
            status = elem.attrib.get("data-batch-status", "")
            init_time = elem.attrib.get("data-init-time", "")
            lead_raw = elem.attrib.get("data-lead-hours")
            try:
                b_ord = int(ord_raw) if ord_raw is not None else -1
            except ValueError:
                b_ord = -1
            if b_ord < 0:
                findings.append(f".evo-batch-col invalid data-batch-ordinal={ord_raw!r}")
            elif b_ord in extracted["batches"]:
                findings.append(f"Duplicate .evo-batch-col data-batch-ordinal={b_ord}")
            else:
                if status not in ("ok", "missing", "failed"):
                    findings.append(f"Batch b{b_ord} invalid data-batch-status={status!r}")
                if not init_time:
                    findings.append(f"Batch b{b_ord} missing data-init-time")
                try:
                    lead_h = int(lead_raw) if lead_raw is not None else None
                except ValueError:
                    lead_h = None
                if lead_h is None:
                    findings.append(f"Batch b{b_ord} invalid data-lead-hours={lead_raw!r}")
                extracted["batches"][b_ord] = {
                    "ordinal": b_ord,
                    "status": status,
                    "init_time": init_time,
                    "lead_hours": lead_h,
                }
                # Validate single batch header inside this valid data-batch-ordinal column
                hdr_id = f"ensabc-batch-header-{b_ord}"
                for b_child in list(elem):
                    if b_child.attrib.get("id") == hdr_id and "evo-batch-header" in _class_tokens(b_child):
                        if b_ord in seen_batch_headers:
                            findings.append(f"Duplicate #{hdr_id}")
                        else:
                            seen_batch_headers.add(b_ord)
                            h_ch = list(b_child)
                            h_rects = [c for c in h_ch if _strip_ns(c.tag) == "rect" and len(list(c)) == 0]
                            h_texts = [c for c in h_ch if _strip_ns(c.tag) == "text" and len(list(c)) == 0]
                            if _strip_ns(b_child.tag) == "g" and len(h_ch) == 3 and len(h_rects) == 1 and len(h_texts) == 2:
                                whitelisted_chrome_primitives.update(h_ch)
                            else:
                                findings.append(f"#{hdr_id} contains unexpected primitives")

        if "evo-gap-marker" in classes:
            ord_raw = elem.attrib.get("data-batch-ordinal")
            g_status = elem.attrib.get("data-status", "")
            g_reason = elem.attrib.get("data-status-reason", "")
            try:
                g_ord = int(ord_raw) if ord_raw is not None else -1
            except ValueError:
                g_ord = -1
            if g_ord < 0:
                findings.append(f".evo-gap-marker invalid data-batch-ordinal={ord_raw!r}")
            elif g_ord in extracted["gap_markers"]:
                findings.append(f"Duplicate .evo-gap-marker data-batch-ordinal={g_ord}")
            else:
                if g_status not in ("missing", "failed"):
                    findings.append(f".evo-gap-marker b{g_ord} invalid data-status={g_status!r}")
                if not g_reason.strip():
                    findings.append(f".evo-gap-marker b{g_ord} empty data-status-reason")
                extracted["gap_markers"][g_ord] = {
                    "ordinal": g_ord,
                    "status": g_status,
                    "status_reason": g_reason,
                }

        if "evo-node" in classes:
            nid = elem.attrib.get("data-node-id", "")
            m = NODE_ID_RE.match(nid)
            if not m:
                findings.append(f".evo-node invalid data-node-id={nid!r}")
            elif nid in extracted["nodes"]:
                findings.append(f"Duplicate .evo-node data-node-id={nid!r}")
            else:
                if elem.attrib.get("tabindex") != "0":
                    findings.append(f".evo-node {nid} missing tabindex='0'")
                req_attrs = (
                    "data-evolution-run-id",
                    "data-batch-index",
                    "data-cluster-id",
                    "data-canonical-rank",
                    "data-branch-id",
                    "data-incoming-role",
                    "data-outgoing-role",
                    "data-member-count",
                    "data-member-fraction",
                    "data-high-uncertainty",
                )
                for ra in req_attrs:
                    if ra not in elem.attrib or elem.attrib[ra] == "":
                        findings.append(f".evo-node {nid} missing attribute {ra}")
                extracted["nodes"][nid] = dict(elem.attrib)
                if int(m.group(1)) != int(elem.attrib.get("data-batch-index", "-999")):
                    findings.append(f".evo-node {nid} batch ordinal disagrees with data-batch-index")
                if int(m.group(2)) != int(elem.attrib.get("data-cluster-id", "-999")):
                    findings.append(f".evo-node {nid} cluster_id disagrees with data-cluster-id")

        if "evo-edge" in classes:
            eid = elem.attrib.get("data-edge-id", "")
            m = EDGE_ID_RE.match(eid)
            if not m:
                findings.append(f".evo-edge invalid data-edge-id={eid!r}")
            elif eid in extracted["edges"]:
                findings.append(f"Duplicate .evo-edge data-edge-id={eid!r}")
            else:
                if elem.attrib.get("tabindex") != "0":
                    findings.append(f".evo-edge {eid} missing tabindex='0'")
                req_attrs = (
                    "data-evolution-run-id",
                    "data-prev-node-id",
                    "data-curr-node-id",
                    "data-edge-role",
                    "data-event-type",
                    "data-distance",
                    "data-distance-over-scale",
                    "data-attributed-flow",
                    "data-ambiguous-tie",
                    "data-high-uncertainty",
                )
                for ra in req_attrs:
                    if ra not in elem.attrib or elem.attrib[ra] == "":
                        findings.append(f".evo-edge {eid} missing attribute {ra}")
                extracted["edges"][eid] = dict(elem.attrib)

        if local_low in GEOMETRIC_PRIMITIVE_TAGS:
            owned = elem in whitelisted_chrome_primitives
            if not owned:
                for anc in ancestors:
                    anc_tag = _strip_ns(anc.tag).lower()
                    anc_cls = _class_tokens(anc)
                    if anc_tag == "defs":
                        owned = True
                        break
                    if "evo-node" in anc_cls and NODE_ID_RE.match(anc.attrib.get("data-node-id", "")):
                        owned = True
                        break
                    if "evo-edge" in anc_cls and EDGE_ID_RE.match(anc.attrib.get("data-edge-id", "")):
                        owned = True
                        break
                    if "evo-gap-marker" in anc_cls and (
                        anc.attrib.get("data-batch-ordinal", "").isdigit()
                        or anc.attrib.get("data-gap-prev-ordinal", "").isdigit()
                    ):
                        owned = True
                        break
            if not owned:
                extracted["orphan_primitives"].append(
                    f"<{local}> attrs={dict(elem.attrib)}"
                )

        for child in list(elem):
            walk(child, chain)

    walk(svg_root, [])

    lane_guides_el = next((c for c in list(svg_root) if c.attrib.get("id") == "ensabc-lane-guides"), None)
    if lane_guides_el is not None and extracted["batches"]:
        if len(list(lane_guides_el)) != len(extracted["batches"]):
            findings.append(
                f"#ensabc-lane-guides line count ({len(list(lane_guides_el))}) != batch count ({len(extracted['batches'])})"
            )

    if extracted["orphan_primitives"]:
        findings.append(
            f"Found {len(extracted['orphan_primitives'])} untagged orphan SVG geometric primitives: "
            f"{extracted['orphan_primitives'][:3]}"
        )

    # Check batch contiguity & gap marker alignment
    if extracted["batches"]:
        expected_ords = list(range(len(extracted["batches"])))
        if sorted(extracted["batches"].keys()) != expected_ords:
            findings.append(
                f"SVG batch ordinals {sorted(extracted['batches'].keys())} != {expected_ords}"
            )
        for b_ord, b_info in extracted["batches"].items():
            if b_info["status"] in ("missing", "failed"):
                if b_ord not in extracted["gap_markers"]:
                    findings.append(f"Batch b{b_ord} ({b_info['status']}) missing .evo-gap-marker")
            else:
                if b_ord in extracted["gap_markers"]:
                    findings.append(f"Batch b{b_ord} (ok) unexpectedly has .evo-gap-marker")

    # Check nodes sit only on ok batches
    for nid, nattrs in extracted["nodes"].items():
        m = NODE_ID_RE.match(nid)
        if m:
            b_ord = int(m.group(1))
            b_st = extracted["batches"].get(b_ord, {}).get("status")
            if b_st != "ok":
                findings.append(f"SVG node {nid} placed in non-ok batch b{b_ord} (status={b_st!r})")

    # Check edge endpoints, adjacent ordinals, and zero cross-gap edges
    for eid, eattrs in extracted["edges"].items():
        m = EDGE_ID_RE.match(eid)
        if not m:
            continue
        p_ord, p_cid, c_ord, c_cid = (int(m.group(i)) for i in range(1, 5))
        expected_prev_nid = f"b{p_ord}:c{p_cid}"
        expected_curr_nid = f"b{c_ord}:c{c_cid}"
        if eattrs.get("data-prev-node-id") != expected_prev_nid:
            findings.append(f"SVG edge {eid} data-prev-node-id={eattrs.get('data-prev-node-id')!r} mismatch")
        if eattrs.get("data-curr-node-id") != expected_curr_nid:
            findings.append(f"SVG edge {eid} data-curr-node-id={eattrs.get('data-curr-node-id')!r} mismatch")
        if c_ord != p_ord + 1:
            findings.append(
                f"SVG edge {eid} connects non-adjacent batches b{p_ord} -> b{c_ord} (cross-gap/multi-hop forbidden)"
            )
        if expected_prev_nid not in extracted["nodes"]:
            findings.append(f"SVG edge {eid} missing source node {expected_prev_nid}")
        if expected_curr_nid not in extracted["nodes"]:
            findings.append(f"SVG edge {eid} missing target node {expected_curr_nid}")
        p_st = extracted["batches"].get(p_ord, {}).get("status")
        c_st = extracted["batches"].get(c_ord, {}).get("status")
        if p_st != "ok" or c_st != "ok":
            findings.append(
                f"SVG edge {eid} touches unavailable/gap batch (b{p_ord}={p_st!r}, b{c_ord}={c_st!r})"
            )

    return (
        {
            "status": "pass" if not findings else "fail",
            "svg_batches_count": len(extracted["batches"]),
            "svg_gap_markers_count": len(extracted["gap_markers"]),
            "svg_nodes_count": len(extracted["nodes"]),
            "svg_edges_count": len(extracted["edges"]),
            "orphan_primitives_count": len(extracted["orphan_primitives"]),
            "findings": findings,
        },
        extracted,
    )


def _extract_provenance_json(dom: _DOMInspectorParser) -> tuple[dict[str, Any], list[str]]:
    prov_scripts = [
        body
        for attrs, body in dom.scripts
        if attrs.get("id") == "ensabc-evolution-provenance"
        and attrs.get("type", "").lower() == "application/json"
    ]
    if len(prov_scripts) != 1:
        raise ValueError(
            f"Expected exactly 1 <script id='ensabc-evolution-provenance' type='application/json'>, found {len(prov_scripts)}"
        )
    raw_body = prov_scripts[0]
    escape_findings: list[str] = []
    if "<" in raw_body or ">" in raw_body:
        escape_findings.append(
            "Raw '<' or '>' character forbidden inside <script id='ensabc-evolution-provenance'> (must be \\u003c / \\u003e escaped)"
        )
    parsed = json.loads(
        raw_body,
        parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Illegal non-finite constant {c}")),
    )
    return parsed, escape_findings


def _verify_ledger_kv_against_run(
    dom: _DOMInspectorParser,
    run_obj: dict[str, Any],
    target_label: str,
    findings: list[str],
) -> None:
    if dom.ledger_duplicate_keys:
        findings.append(
            f"#ensabc-provenance-ledger duplicate key rows: {dom.ledger_duplicate_keys}"
        )
    expected_kv = (
        ("evolution_run_id", str(run_obj.get("evolution_run_id", ""))),
        ("tracking_run_key", str(run_obj.get("tracking_run_key", ""))),
        ("content_sha256", str(run_obj.get("content_sha256", ""))),
        ("evolution_algorithm_version", str(run_obj.get("algorithm_version", ""))),
        ("secondary_rule", str(run_obj.get("secondary_rule", ""))),
        ("flow_rule", str(run_obj.get("flow_rule", ""))),
        ("uncertainty_rule", str(run_obj.get("uncertainty_rule", ""))),
    )
    for k_name, exp_val in expected_kv:
        act_val = dom.ledger_kv.get(k_name)
        if act_val != exp_val:
            findings.append(
                f"#ensabc-provenance-ledger[{k_name!r}]={act_val!r} != {target_label} {exp_val!r}"
            )


def _pass_3_svg_json_bijection(
    dom: _DOMInspectorParser,
    svg_data: dict[str, Any],
    expected_run_id: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    findings: list[str] = []
    try:
        manifest, escape_findings = _extract_provenance_json(dom)
        findings.extend(escape_findings)
    except Exception as exc:
        return {"status": "fail", "findings": [f"Embedded JSON parse failure: {exc}"]}, None

    if not isinstance(manifest, dict):
        return {"status": "fail", "findings": ["Embedded provenance payload is not a JSON object"]}, None

    if manifest.get("schema_version") != SCHEMA_VERSION:
        findings.append(
            f"Embedded schema_version={manifest.get('schema_version')!r}, expected {SCHEMA_VERSION!r}"
        )

    for top_k in ("run", "disclaimer", "batches", "nodes", "edges", "transitions", "branches"):
        if top_k not in manifest:
            findings.append(f"Embedded JSON missing top-level section {top_k!r}")
    if findings:
        return {"status": "fail", "findings": findings}, manifest

    run_obj = manifest["run"]
    if run_obj.get("evolution_run_id") != expected_run_id:
        findings.append(
            f"Embedded run.evolution_run_id={run_obj.get('evolution_run_id')!r} != expected {expected_run_id!r}"
        )

    json_nodes = manifest["nodes"]
    json_edges = manifest["edges"]
    json_batches = manifest["batches"]
    if not isinstance(json_nodes, dict) or not isinstance(json_edges, dict) or not isinstance(json_batches, list):
        findings.append("Invalid types for json batches/nodes/edges")
        return {"status": "fail", "findings": findings}, manifest

    # Required keys check on JSON nodes and edges (catches stripped margins / rule_basis)
    req_node_keys = {
        "node_id",
        "evolution_run_id",
        "ordinal",
        "cluster_id",
        "canonical_rank",
        "branch_id",
        "mean_features",
        "member_count",
        "member_fraction",
        "assignment_reason",
        "in_degree_primary",
        "in_degree_secondary",
        "out_degree_primary",
        "out_degree_secondary",
        "incoming_role",
        "outgoing_role",
        "primary_delta_fraction",
        "has_primary_delta_fraction",
        "unattributed_inflow_fraction",
        "unattributed_outflow_fraction",
        "high_uncertainty",
    }
    req_edge_keys = {
        "edge_id",
        "evolution_run_id",
        "prev_node_id",
        "curr_node_id",
        "prev_ordinal",
        "prev_cluster_id",
        "curr_ordinal",
        "curr_cluster_id",
        "prev_branch_id",
        "curr_branch_id",
        "edge_role",
        "event_type",
        "rule_basis",
        "distance",
        "distance_over_scale",
        "prev_member_fraction",
        "curr_member_fraction",
        "edge_delta_fraction",
        "attributed_flow",
        "edge_strength",
        "threshold_margin",
        "threshold_margin_ratio",
        "primary_alternative_status",
        "has_optimality_margin",
        "optimality_margin",
        "optimality_margin_ratio",
        "has_secondary_margin",
        "secondary_margin",
        "secondary_margin_ratio",
        "ambiguous_tie",
        "high_uncertainty",
        "uncertainty_reasons",
        "non_physical_disclaimer",
    }

    for nid, nd in json_nodes.items():
        if not isinstance(nd, dict):
            findings.append(f"JSON node {nid} is not an object")
            continue
        missing = sorted(req_node_keys - set(nd.keys()))
        if missing:
            findings.append(f"JSON node {nid} missing required keys: {missing}")
        for mk in req_node_keys & set(nd.keys()):
            if nd[mk] is None or (isinstance(nd[mk], str) and not nd[mk].strip()):
                findings.append(f"JSON node {nid}.{mk} is NULL or empty")

    for eid, ed in json_edges.items():
        if not isinstance(ed, dict):
            findings.append(f"JSON edge {eid} is not an object")
            continue
        missing = sorted(req_edge_keys - set(ed.keys()))
        if missing:
            findings.append(f"JSON edge {eid} missing required keys: {missing}")
        for mk in req_edge_keys & set(ed.keys()):
            if ed[mk] is None or (isinstance(ed[mk], str) and not ed[mk].strip()):
                findings.append(f"JSON edge {eid}.{mk} is NULL or empty")

    # Bijection checks: batches, gap markers, nodes, edges
    svg_nodes = svg_data["nodes"]
    svg_edges = svg_data["edges"]
    svg_batches = svg_data["batches"]
    svg_gaps = svg_data["gap_markers"]

    json_batch_by_ord = {b.get("ordinal"): b for b in json_batches if isinstance(b, dict)}
    if set(svg_batches.keys()) != set(json_batch_by_ord.keys()):
        findings.append(
            f"Batch ordinal bijection mismatch: svg={sorted(svg_batches.keys())} vs json={sorted(json_batch_by_ord.keys())}"
        )
    for b_ord in sorted(set(svg_batches.keys()) & set(json_batch_by_ord.keys())):
        sb = svg_batches[b_ord]
        jb = json_batch_by_ord[b_ord]
        if sb["status"] != jb.get("status"):
            findings.append(f"Batch b{b_ord} status mismatch svg={sb['status']!r} vs json={jb.get('status')!r}")
        if sb["init_time"] != jb.get("init_time"):
            findings.append(f"Batch b{b_ord} init_time mismatch svg={sb['init_time']!r} vs json={jb.get('init_time')!r}")
        if sb["lead_hours"] != jb.get("lead_hours"):
            findings.append(f"Batch b{b_ord} lead_hours mismatch svg={sb['lead_hours']!r} vs json={jb.get('lead_hours')!r}")

    expected_gaps = {
        b_ord: jb for b_ord, jb in json_batch_by_ord.items() if jb.get("status") in ("missing", "failed")
    }
    if set(svg_gaps.keys()) != set(expected_gaps.keys()):
        findings.append(
            f"Gap marker bijection mismatch: svg={sorted(svg_gaps.keys())} vs json={sorted(expected_gaps.keys())}"
        )
    for g_ord in sorted(set(svg_gaps.keys()) & set(expected_gaps.keys())):
        sg = svg_gaps[g_ord]
        jg = expected_gaps[g_ord]
        if sg["status"] != jg.get("status") or sg["status_reason"] != jg.get("status_reason"):
            findings.append(f"Gap marker b{g_ord} status/reason mismatch between SVG and JSON")

    if set(svg_nodes.keys()) != set(json_nodes.keys()):
        only_svg = sorted(set(svg_nodes.keys()) - set(json_nodes.keys()))
        only_json = sorted(set(json_nodes.keys()) - set(svg_nodes.keys()))
        findings.append(f"Node key bijection mismatch: only_in_svg={only_svg}, only_in_json={only_json}")

    for nid in sorted(set(svg_nodes.keys()) & set(json_nodes.keys())):
        sn = svg_nodes[nid]
        jn = json_nodes[nid]
        if not isinstance(jn, dict):
            continue
        str_pairs = (
            ("data-evolution-run-id", str(jn.get("evolution_run_id", ""))),
            ("data-batch-index", str(jn.get("ordinal", ""))),
            ("data-cluster-id", str(jn.get("cluster_id", ""))),
            ("data-canonical-rank", str(jn.get("canonical_rank", ""))),
            ("data-branch-id", str(jn.get("branch_id", ""))),
            ("data-incoming-role", str(jn.get("incoming_role", ""))),
            ("data-outgoing-role", str(jn.get("outgoing_role", ""))),
            ("data-member-count", str(jn.get("member_count", ""))),
            ("data-high-uncertainty", "true" if jn.get("high_uncertainty") is True else "false"),
        )
        for attr_k, exp_str in str_pairs:
            if sn.get(attr_k) != exp_str:
                findings.append(f"Node {nid} {attr_k}={sn.get(attr_k)!r} != JSON {exp_str!r}")
        if sn.get("data-evolution-run-id") != expected_run_id:
            findings.append(f"Node {nid} data-evolution-run-id != {expected_run_id!r}")
        try:
            svg_mf = float(sn.get("data-member-fraction", "nan"))
        except ValueError:
            svg_mf = float("nan")
        if not _floats_equal(svg_mf, jn.get("member_fraction", float("nan"))):
            findings.append(
                f"Node {nid} data-member-fraction={sn.get('data-member-fraction')!r} != JSON {jn.get('member_fraction')!r}"
            )

    if set(svg_edges.keys()) != set(json_edges.keys()):
        only_svg = sorted(set(svg_edges.keys()) - set(json_edges.keys()))
        only_json = sorted(set(json_edges.keys()) - set(svg_edges.keys()))
        findings.append(f"Edge key bijection mismatch: only_in_svg={only_svg}, only_in_json={only_json}")

    for eid in sorted(set(svg_edges.keys()) & set(json_edges.keys())):
        se = svg_edges[eid]
        je = json_edges[eid]
        if not isinstance(je, dict):
            continue
        str_pairs = (
            ("data-evolution-run-id", str(je.get("evolution_run_id", ""))),
            ("data-prev-node-id", str(je.get("prev_node_id", ""))),
            ("data-curr-node-id", str(je.get("curr_node_id", ""))),
            ("data-edge-role", str(je.get("edge_role", ""))),
            ("data-event-type", str(je.get("event_type", ""))),
            ("data-ambiguous-tie", "true" if je.get("ambiguous_tie") is True else "false"),
            ("data-high-uncertainty", "true" if je.get("high_uncertainty") is True else "false"),
        )
        for attr_k, exp_str in str_pairs:
            if se.get(attr_k) != exp_str:
                findings.append(f"Edge {eid} {attr_k}={se.get(attr_k)!r} != JSON {exp_str!r}")
        if se.get("data-evolution-run-id") != expected_run_id:
            findings.append(f"Edge {eid} data-evolution-run-id != {expected_run_id!r}")
        for attr_k, json_k in (
            ("data-distance", "distance"),
            ("data-distance-over-scale", "distance_over_scale"),
            ("data-attributed-flow", "attributed_flow"),
        ):
            try:
                sval = float(se.get(attr_k, "nan"))
            except ValueError:
                sval = float("nan")
            if not _floats_equal(sval, je.get(json_k, float("nan"))):
                findings.append(
                    f"Edge {eid} {attr_k}={se.get(attr_k)!r} != JSON {json_k}={je.get(json_k)!r}"
                )

    # Check #ensabc-inspector-drawer, #ensabc-provenance-ledger, and interactive ES6 bindings
    if not dom.inspector_drawer_found:
        findings.append("Missing <aside id='ensabc-inspector-drawer'>")
    if not dom.provenance_ledger_found:
        findings.append("Missing <details id='ensabc-provenance-ledger'>")
    else:
        _verify_ledger_kv_against_run(dom, run_obj, "provenance_json.run", findings)

    inline_js = "\n".join(
        body for attrs, body in dom.scripts if attrs.get("type", "").lower() != "application/json"
    )
    for token in (
        "ensabc-inspector-content",
        "mouseenter",
        "click",
        "keydown",
        "Enter",
        "rule_basis",
        "optimality_margin",
        "secondary_margin",
        "threshold_margin",
        "attributed_flow",
        "uncertainty_reasons",
    ):
        if token not in inline_js:
            findings.append(f"Interactive inspector script missing required binding/field token {token!r}")

    return {"status": "pass" if not findings else "fail", "findings": findings}, manifest


def _pass_4_duckdb_ground_truth(
    db_path: Path,
    expected_run_id: str,
    manifest: dict[str, Any] | None,
    svg_data: dict[str, Any],
    dom: _DOMInspectorParser | None = None,
) -> dict[str, Any]:
    findings: list[str] = []
    try:
        raw_db = _fetch_evolution_history_packed(db_path, expected_run_id)
        db_manifest = build_view_manifest(raw_db, expected_run_id)
    except MissingProvenanceError as exc:
        return {"status": "fail", "findings": [f"DuckDB ground-truth load failed: {exc}"]}
    except Exception as exc:
        return {"status": "fail", "findings": [f"DuckDB ground-truth unexpected error: {exc}"]}

    if manifest is None:
        return {"status": "fail", "findings": ["Embedded JSON unavailable for DuckDB comparison"]}

    if dom is not None and dom.provenance_ledger_found:
        _verify_ledger_kv_against_run(dom, db_manifest["run"], "db_manifest.run", findings)

    # Full 100% recursive field-by-field comparison against DuckDB ground-truth manifest
    for section in ("schema_version", "run", "batches", "nodes", "edges", "transitions", "branches"):
        _deep_compare(manifest.get(section), db_manifest.get(section), f"manifest.{section}", findings)

    # Direct SVG <-> DuckDB check (defending against simultaneous SVG + JSON tampering)
    db_nodes = db_manifest["nodes"]
    db_edges = db_manifest["edges"]
    if set(svg_data["nodes"].keys()) != set(db_nodes.keys()):
        findings.append("SVG node keys disagree with DuckDB evolution_nodes")
    for nid in sorted(set(svg_data["nodes"].keys()) & set(db_nodes.keys())):
        sn = svg_data["nodes"][nid]
        dn = db_nodes[nid]
        if sn.get("data-branch-id") != dn["branch_id"]:
            findings.append(f"SVG node {nid} branch_id != DuckDB ({dn['branch_id']!r})")
        if sn.get("data-incoming-role") != dn["incoming_role"] or sn.get("data-outgoing-role") != dn["outgoing_role"]:
            findings.append(f"SVG node {nid} role != DuckDB")
        try:
            smf = float(sn.get("data-member-fraction", "nan"))
        except ValueError:
            smf = float("nan")
        if not _floats_equal(smf, dn["member_fraction"]):
            findings.append(f"SVG node {nid} member_fraction != DuckDB ({dn['member_fraction']!r})")

    if set(svg_data["edges"].keys()) != set(db_edges.keys()):
        findings.append("SVG edge keys disagree with DuckDB evolution_edges")
    for eid in sorted(set(svg_data["edges"].keys()) & set(db_edges.keys())):
        se = svg_data["edges"][eid]
        de = db_edges[eid]
        if se.get("data-edge-role") != de["edge_role"]:
            findings.append(
                f"SVG edge {eid} data-edge-role={se.get('data-edge-role')!r} != DuckDB {de['edge_role']!r}"
            )
        if se.get("data-event-type") != de["event_type"]:
            findings.append(
                f"SVG edge {eid} data-event-type={se.get('data-event-type')!r} != DuckDB {de['event_type']!r}"
            )
        for attr_k, db_k in (
            ("data-distance", "distance"),
            ("data-distance-over-scale", "distance_over_scale"),
            ("data-attributed-flow", "attributed_flow"),
        ):
            try:
                sval = float(se.get(attr_k, "nan"))
            except ValueError:
                sval = float("nan")
            if not _floats_equal(sval, de[db_k]):
                findings.append(
                    f"SVG edge {eid} {attr_k}={se.get(attr_k)!r} != DuckDB {db_k}={de[db_k]!r}"
                )

    return {
        "status": "pass" if not findings else "fail",
        "db_nodes_verified": len(db_nodes),
        "db_edges_verified": len(db_edges),
        "db_events_verified": sum(len(tr["events"]) for tr in db_manifest["transitions"]),
        "db_transitions_verified": len(db_manifest["transitions"]),
        "db_branches_verified": len(db_manifest["branches"]),
        "findings": findings,
    }


_CSS_HIDING_DECL_RE = re.compile(
    r"display\s*:\s*none"
    r"|visibility\s*:\s*(?:hidden|collapse)"
    r"|opacity\s*:\s*(?:0+(?:\.0*)?|\.0+)(?![0-9.])"
    r"|font-size\s*:\s*(?:0+(?:\.0*)?|\.0+)(?:[a-z%]+)?(?![0-9.])"
    r"|(?:max-)?(?:height|width)\s*:\s*(?:0+(?:\.0*)?|\.0+)(?:px|rem|em|%|vh|vw|ch|ex|pt|cm|mm|in)?\s*(?:;|$|!)"
    r"|transform\s*:\s*[^;{}]*scale[xy3d]*\(\s*(?:0+(?:\.0*)?|\.0+)\s*(?:[,)])"
    r"|clip-path\s*:\s*inset\s*\(\s*100%"
    r"|color\s*:\s*transparent\b"
    r"|color\s*:\s*rgba\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*(?:0+(?:\.0*)?|\.0+)\s*\)",
    flags=re.IGNORECASE,
)


def _has_hiding_css_declarations(style_or_block: str) -> bool:
    cleaned = _strip_css_comments(style_or_block)
    return _CSS_HIDING_DECL_RE.search(cleaned) is not None


def _selector_targets_disclaimer(selectors: str) -> bool:
    sel_low = selectors.lower()
    if "disclaimer" in sel_low or "data-clause-id" in sel_low:
        return True
    return re.search(r"(?:^|,|\s)(?:\*|html|body|header)(?:\s|,|:|\[|$)", sel_low) is not None


def _css_hides_disclaimer(css_text: str) -> bool:
    cleaned = _strip_css_comments(css_text)
    for block_match in re.finditer(r"([^{}]+)\{([^{}]*)\}", cleaned):
        selectors = block_match.group(1)
        body = block_match.group(2)
        if _selector_targets_disclaimer(selectors) and _has_hiding_css_declarations(body):
            return True
    return False


def scan_prohibited_claims(text: str) -> list[str]:
    hits: list[str] = []
    for rule_id, pat in PROHIBITED_CLAIM_PATTERNS:
        m = pat.search(text)
        if m:
            snippet = m.group(0).strip()
            hits.append(f"Prohibited claim [{rule_id}]: {snippet!r}")
    return hits


def _collect_json_strings(obj: Any, acc: list[str]) -> None:
    if isinstance(obj, str):
        acc.append(obj)
    elif isinstance(obj, list):
        for v in obj:
            _collect_json_strings(v, acc)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_json_strings(v, acc)


def _pass_5_disclaimer_and_claims(
    dom: _DOMInspectorParser,
    manifest: dict[str, Any] | None,
) -> dict[str, Any]:
    findings: list[str] = []
    banner = dom.banner_info
    if banner is None:
        findings.append("Missing <header id='ensabc-disclaimer-banner'>")
    else:
        if dom.banner_count != 1:
            findings.append(f"Expected exactly 1 #ensabc-disclaimer-banner, found {dom.banner_count}")
        if banner["tag"] != "header":
            findings.append(f"#ensabc-disclaimer-banner must be <header>, got <{banner['tag']}>")
        attrs = banner["attrs"]
        if attrs.get("data-disclaimer-version") != DISCLAIMER_VERSION:
            findings.append(
                f"data-disclaimer-version={attrs.get('data-disclaimer-version')!r} != {DISCLAIMER_VERSION!r}"
            )
        if "hidden" in attrs or attrs.get("aria-hidden", "").strip().lower() == "true":
            findings.append("#ensabc-disclaimer-banner has hidden / aria-hidden='true' attribute")
        inline_style = attrs.get("style", "")
        if _has_hiding_css_declarations(inline_style):
            findings.append(f"#ensabc-disclaimer-banner hidden via inline style: {inline_style!r}")

        for anc_tag, anc_attrs in banner["ancestors"]:
            if anc_tag == "details":
                findings.append("#ensabc-disclaimer-banner must be non-collapsible (cannot reside inside <details>)")
            if "hidden" in anc_attrs or anc_attrs.get("aria-hidden", "").strip().lower() == "true":
                findings.append(f"Ancestor <{anc_tag}> of #ensabc-disclaimer-banner is hidden")
            anc_style = anc_attrs.get("style", "")
            if _has_hiding_css_declarations(anc_style):
                findings.append(f"Ancestor <{anc_tag}> hides #ensabc-disclaimer-banner via inline style")

        for el in dom.elements:
            if any(anc_attrs.get("id") == "ensabc-disclaimer-banner" for _, anc_attrs in el["ancestors"]):
                d_tag = el["tag"]
                d_attrs = el["attrs"]
                if d_tag == "details":
                    findings.append("Descendant <details> wrapper forbidden inside #ensabc-disclaimer-banner")
                if "hidden" in d_attrs or d_attrs.get("aria-hidden", "").strip().lower() == "true":
                    findings.append(
                        f"Descendant <{d_tag}> inside #ensabc-disclaimer-banner has hidden / aria-hidden='true'"
                    )
                d_style = d_attrs.get("style", "")
                if _has_hiding_css_declarations(d_style):
                    findings.append(
                        f"Descendant <{d_tag}> inside #ensabc-disclaimer-banner hidden via inline style: {d_style!r}"
                    )

        for idx, css in enumerate(dom.styles):
            if _css_hides_disclaimer(css):
                findings.append(f"<style>[{idx}] hides #ensabc-disclaimer-banner via CSS rule")

        for idx, (s_attrs, js) in enumerate(dom.scripts):
            if s_attrs.get("type", "").lower() != "application/json" and "ensabc-disclaimer-banner" in js:
                findings.append(f"<script>[{idx}] references #ensabc-disclaimer-banner (JS mutation forbidden)")

        btext = banner.get("text", "")
        btext_low = btext.lower()
        # Verify all 3 mandatory clauses in visible banner text
        if not (
            ("非物理成员迁移" in btext or "not represent physical ensemble member migration" in btext_low)
            and "attributed_flow" in btext
        ):
            findings.append("Disclaimer banner missing Clause 1 (non-physical member migration & attributed_flow)")
        if not (
            ("非物理新生" in btext or "not represent physical cyclogenesis" in btext_low)
            and ("emergence" in btext_low and "disappearance" in btext_low)
        ):
            findings.append("Disclaimer banner missing Clause 2 (non-physical cyclogenesis/lysis)")
        if not (
            ("未经概率校准" in btext or "not a calibrated probability" in btext_low)
            and ("交易" in btext or "trading" in btext_low)
        ):
            findings.append(
                "Disclaimer banner missing Clause 3 (non-calibrated probability / forecast skill / trading value)"
            )

    if isinstance(manifest, dict):
        disc = manifest.get("disclaimer")
        if not isinstance(disc, dict):
            findings.append("Embedded JSON missing disclaimer object")
        else:
            if disc.get("version") != DISCLAIMER_VERSION:
                findings.append(f"JSON disclaimer.version={disc.get('version')!r} != {DISCLAIMER_VERSION!r}")
            if disc.get("non_physical_token") != NON_PHYSICAL_TOKEN:
                findings.append("JSON disclaimer.non_physical_token mismatch")
            clauses = disc.get("clauses")
            if not isinstance(clauses, list) or len(clauses) < 3:
                findings.append("JSON disclaimer.clauses must list all 3 mandatory clauses")
            else:
                clause_ids = {c.get("id") for c in clauses if isinstance(c, dict)}
                expected_ids = {
                    "non_member_migration",
                    "non_physical_cyclogenesis",
                    "non_calibrated_probability",
                }
                if not expected_ids.issubset(clause_ids):
                    findings.append(f"JSON disclaimer.clauses missing IDs: {sorted(expected_ids - clause_ids)}")

    # Scan visible text and embedded JSON strings for prohibited affirmative claims
    visible_corpus = "\n".join(dom.visible_text_chunks)
    findings.extend(scan_prohibited_claims(visible_corpus))
    if manifest is not None:
        json_strings: list[str] = []
        _collect_json_strings(manifest, json_strings)
        findings.extend(scan_prohibited_claims("\n".join(json_strings)))

    return {"status": "pass" if not findings else "fail", "findings": findings}


def verify_html_traceability(
    html_path: Path,
    db_path: Path,
    evolution_run_id: str,
    *,
    out_report: Path | None = None,
) -> dict[str, Any]:
    """Run Passes 1-5 on an M3 visualization HTML file against its DuckDB ground truth."""
    html_text = html_path.read_text(encoding="utf-8")
    dom = _DOMInspectorParser()
    dom.feed(html_text)
    dom.close()

    p1 = _pass_1_offline_csp(html_text, dom)
    p2, svg_data = _pass_2_svg_xml_and_zero_orphans(html_text, dom)
    p3, manifest = _pass_3_svg_json_bijection(dom, svg_data, evolution_run_id)
    p4 = _pass_4_duckdb_ground_truth(db_path, evolution_run_id, manifest, svg_data, dom)
    p5 = _pass_5_disclaimer_and_claims(dom, manifest)

    passes = {
        "pass_1_offline_csp": p1,
        "pass_2_svg_xml_zero_orphans": p2,
        "pass_3_svg_json_bijection": p3,
        "pass_4_duckdb_ground_truth": p4,
        "pass_5_disclaimer_and_claims": p5,
    }
    all_findings: list[str] = []
    for pname, pres in passes.items():
        for item in pres["findings"]:
            all_findings.append(f"[{pname}] {item}")

    overall_pass = all(pres["status"] == "pass" for pres in passes.values())
    report = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "status": "pass" if overall_pass else "fail",
        "audited_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evolution_run_id": evolution_run_id,
        "html_path": str(html_path),
        "html_sha256": sha256_file(html_path),
        "db_path": str(db_path),
        "passes": passes,
        "findings": all_findings,
    }
    if out_report is not None:
        atomic_write_json(out_report, report)
    return report


# ---------------------------------------------------------------------------
# Evidence & Identity Gate Audit Contract Helpers (used by T036 negative tests)
# ---------------------------------------------------------------------------


def verify_branch_id_run_prefix(branch_id: str, tracking_run_key: str) -> None:
    """Verify M2/M3 branch_id carries the authentic `<tracking_run_key[:12]>:b<ord:02d>:k<rank:02d>` prefix."""
    if len(tracking_run_key) < 12:
        raise ValueError(f"Invalid tracking_run_key (length < 12): {tracking_run_key!r}")
    expected_prefix = f"{tracking_run_key[:12]}:"
    if not branch_id.startswith(expected_prefix):
        raise ValueError(
            f"Forged or mismatched branch_id prefix in {branch_id!r}: expected {expected_prefix!r}"
        )
    if not re.match(r"^[0-9a-zA-Z_-]{12}:b\d{2}:k\d{2}$", branch_id):
        raise ValueError(f"Malformed branch_id canonical coordinates: {branch_id!r}")


def audit_evidence_bundle_contract(
    run_dir: Path,
    report_doc: dict[str, Any],
    *,
    required_steps: set[str],
) -> list[str]:
    """Audit gate report evidence paths, SHA-256 digests, execution steps, and prohibited claims."""
    resolved_root = run_dir.resolve()
    findings: list[str] = []

    steps = report_doc.get("steps")
    if not isinstance(steps, list):
        findings.append("Missing execution steps list")
    else:
        for req_step in sorted(required_steps):
            ok = any(
                isinstance(s, dict)
                and s.get("step") == req_step
                and s.get("status") == "pass"
                and s.get("exit_code") == 0
                for s in steps
            )
            if not ok:
                findings.append(f"Missing or failed required gate step: {req_step}")

    for sec_name in ("requirements", "success_criteria"):
        sec = report_doc.get(sec_name, {})
        if not isinstance(sec, dict):
            findings.append(f"Invalid section {sec_name}")
            continue
        for item_id, entry in sec.items():
            if not isinstance(entry, dict):
                continue
            refs: list[tuple[str, str]] = []
            if isinstance(entry.get("evidence_refs"), list):
                for ref in entry["evidence_refs"]:
                    if isinstance(ref, dict):
                        refs.append((str(ref.get("path", "")), str(ref.get("sha256", ""))))
            elif "evidence_path" in entry:
                refs.append((str(entry.get("evidence_path", "")), str(entry.get("evidence_sha256", ""))))

            for rel_path, exp_sha in refs:
                if not rel_path or Path(rel_path).is_absolute() or ".." in Path(rel_path).parts:
                    findings.append(f"{item_id} evidence path traversal or invalid path: {rel_path!r}")
                    continue
                try:
                    ensure_no_symlink_inside(resolved_root, resolved_root / rel_path)
                except Exception as exc:
                    findings.append(f"{item_id} evidence path symlink blocked: {exc}")
                    continue
                target = (resolved_root / rel_path).resolve()
                if not target.is_relative_to(resolved_root):
                    findings.append(f"{item_id} evidence path escapes run_dir: {rel_path!r}")
                    continue
                if not target.is_file():
                    findings.append(f"{item_id} evidence file missing: {rel_path!r}")
                    continue
                actual_sha = sha256_file(target)
                if actual_sha != exp_sha:
                    findings.append(
                        f"{item_id} SHA-256 mismatch on {rel_path}: expected {exp_sha}, got {actual_sha}"
                    )

    text_blobs: list[str] = []
    _collect_json_strings(report_doc, text_blobs)
    findings.extend(scan_prohibited_claims("\n".join(text_blobs)))
    return findings


# ---------------------------------------------------------------------------
# 8-Batch 11-Table Showcase Seeder & 10-Case Tamper Self-Check (Pass 6)
# ---------------------------------------------------------------------------

SHOWCASE_RUN_ID = "e3f4a5b6c7d809102132435465768798a9bacbdcedfe0f112233445566778899"
SHOWCASE_TRACKING_KEY = "a1b2c3d4e5f60718"


def seed_showcase_duckdb(
    db_path: Path,
    *,
    evolution_run_id: str = SHOWCASE_RUN_ID,
    tracking_run_key: str = SHOWCASE_TRACKING_KEY,
) -> dict[str, Any]:
    """Seed all M2 (5 tables) + M3 (6 tables) for an 8-batch showcase covering every event & gap type.

    Covers:
    - b0 -> b1 (`ok`): `continuation` + `split` (`primary` + `secondary_split`) + `disappearance` + `emergence`
    - b1 -> b2 (`ok`): `continuation` + `merge` (`primary` + `secondary_merge`, exact `ambiguous_tie = true`)
                       + near-threshold `continuation` (`high_uncertainty = true`)
    - b3 (`missing`): `gap_interrupted` on b2->b3 and `gap_restarted` on b3->b4 (0 cross-gap edges)
    - b4 -> b5 (`ok`): clean continuations
    - b6 (`failed`, `EMPTY_CLUSTER`): `gap_interrupted` on b5->b6 and `gap_restarted` on b6->b7 (0 cross-gap edges)
    """
    pfx = tracking_run_key[:12]
    m2_schema_path = REPO_ROOT / "validation" / "m2" / "sql" / "schema.sql"
    m2_ddl = m2_schema_path.read_text(encoding="utf-8")

    sql = f"""
    {m2_ddl}

    CREATE TABLE IF NOT EXISTS evolution_runs (
        evolution_run_id        VARCHAR PRIMARY KEY,
        tracking_run_key        VARCHAR NOT NULL REFERENCES tracking_runs(run_key),
        evolution_binding       STRUCT(
                                  tracking_run_key VARCHAR,
                                  input_manifest_sha256 VARCHAR,
                                  scenario_config_sha256 VARCHAR,
                                  match_config_sha256 VARCHAR,
                                  evolve_config_sha256 VARCHAR,
                                  weights_version VARCHAR,
                                  secondary_rule VARCHAR,
                                  flow_rule VARCHAR,
                                  uncertainty_rule VARCHAR,
                                  fraction_sum_tolerance DOUBLE,
                                  high_uncertainty_threshold_ratio DOUBLE,
                                  high_uncertainty_optimality_ratio DOUBLE,
                                  high_uncertainty_secondary_ratio DOUBLE,
                                  software STRUCT(
                                    ensabc_extension_sha256 VARCHAR,
                                    duckomo_version VARCHAR,
                                    duckomo_sha256 VARCHAR,
                                    duckdb_version VARCHAR,
                                    cluster_algorithm_version VARCHAR,
                                    match_algorithm_version VARCHAR,
                                    evolution_algorithm_version VARCHAR
                                  )
                                ) NOT NULL,
        weights                 DOUBLE[] NOT NULL,
        planned_batch_count     INTEGER NOT NULL CHECK (planned_batch_count BETWEEN 4 AND 8),
        chain_summary           STRUCT(
                                  ok_batch_count INTEGER,
                                  missing_batch_count INTEGER,
                                  failed_batch_count INTEGER,
                                  ok_transition_count INTEGER,
                                  gap_transition_count INTEGER,
                                  branch_count INTEGER,
                                  primary_edge_count INTEGER,
                                  secondary_split_edge_count INTEGER,
                                  secondary_merge_edge_count INTEGER,
                                  continuation_count INTEGER,
                                  split_count INTEGER,
                                  merge_count INTEGER,
                                  emergence_count INTEGER,
                                  disappearance_count INTEGER,
                                  gap_interrupted_count INTEGER,
                                  gap_restarted_count INTEGER,
                                  cumulative_primary_distance DOUBLE,
                                  max_edge_distance DOUBLE,
                                  max_edge_distance_over_scale DOUBLE,
                                  mean_transition_tv_redistribution DOUBLE,
                                  max_transition_tv_redistribution DOUBLE,
                                  ambiguous_transition_count INTEGER,
                                  high_uncertainty_transition_count INTEGER,
                                  high_uncertainty_transition_ratio DOUBLE,
                                  high_uncertainty_edge_count INTEGER,
                                  high_uncertainty_edge_ratio DOUBLE
                                ) NOT NULL,
        non_physical_disclaimer VARCHAR NOT NULL CHECK (length(non_physical_disclaimer) > 0),
        content_sha256          VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS evolution_nodes (
        evolution_run_id              VARCHAR NOT NULL REFERENCES evolution_runs(evolution_run_id),
        ordinal                       INTEGER NOT NULL CHECK (ordinal >= 0),
        cluster_id                    INTEGER NOT NULL CHECK (cluster_id >= 0),
        canonical_rank                INTEGER NOT NULL CHECK (canonical_rank >= 0),
        branch_id                     VARCHAR NOT NULL,
        mean_features                 DOUBLE[] NOT NULL,
        member_count                  INTEGER NOT NULL CHECK (member_count > 0),
        member_fraction               DOUBLE NOT NULL CHECK (isfinite(member_fraction) AND member_fraction > 0 AND member_fraction <= 1),
        assignment_reason             VARCHAR NOT NULL,
        in_degree_primary             INTEGER NOT NULL CHECK (in_degree_primary IN (0, 1)),
        in_degree_secondary           INTEGER NOT NULL CHECK (in_degree_secondary >= 0),
        out_degree_primary            INTEGER NOT NULL CHECK (out_degree_primary IN (0, 1)),
        out_degree_secondary          INTEGER NOT NULL CHECK (out_degree_secondary >= 0),
        incoming_role                 VARCHAR NOT NULL,
        outgoing_role                 VARCHAR NOT NULL,
        primary_delta_fraction        DOUBLE NOT NULL CHECK (isfinite(primary_delta_fraction)),
        has_primary_delta_fraction    BOOLEAN NOT NULL,
        unattributed_inflow_fraction  DOUBLE NOT NULL CHECK (isfinite(unattributed_inflow_fraction) AND unattributed_inflow_fraction >= 0),
        unattributed_outflow_fraction DOUBLE NOT NULL CHECK (isfinite(unattributed_outflow_fraction) AND unattributed_outflow_fraction >= 0),
        high_uncertainty              BOOLEAN NOT NULL,
        PRIMARY KEY (evolution_run_id, ordinal, cluster_id),
        UNIQUE (evolution_run_id, ordinal, branch_id),
        UNIQUE (evolution_run_id, ordinal, canonical_rank)
    );

    CREATE TABLE IF NOT EXISTS evolution_transitions (
        evolution_run_id              VARCHAR NOT NULL REFERENCES evolution_runs(evolution_run_id),
        prev_ordinal                  INTEGER NOT NULL CHECK (prev_ordinal >= 0),
        curr_ordinal                  INTEGER NOT NULL,
        transition_status             VARCHAR NOT NULL CHECK (transition_status IN ('ok', 'gap')),
        chain_broken                  BOOLEAN NOT NULL,
        gap_reason                    VARCHAR,
        pair_scale                    DOUBLE CHECK (pair_scale IS NULL OR (isfinite(pair_scale) AND pair_scale > 0)),
        threshold                     DOUBLE CHECK (threshold IS NULL OR (isfinite(threshold) AND threshold >= 0)),
        primary_edge_count            INTEGER NOT NULL CHECK (primary_edge_count >= 0),
        secondary_split_edge_count    INTEGER NOT NULL CHECK (secondary_split_edge_count >= 0),
        secondary_merge_edge_count    INTEGER NOT NULL CHECK (secondary_merge_edge_count >= 0),
        transition_tv_redistribution  DOUBLE CHECK (transition_tv_redistribution IS NULL OR (isfinite(transition_tv_redistribution) AND transition_tv_redistribution >= 0)),
        min_threshold_margin          DOUBLE CHECK (min_threshold_margin IS NULL OR (isfinite(min_threshold_margin) AND min_threshold_margin >= 0)),
        ambiguous_tie                 BOOLEAN NOT NULL,
        high_uncertainty              BOOLEAN NOT NULL,
        evolve_result                 STRUCT(
                                        algorithm_version VARCHAR,
                                        summary STRUCT(
                                          primary_edge_count BIGINT,
                                          secondary_split_edge_count BIGINT,
                                          secondary_merge_edge_count BIGINT,
                                          transition_tv_redistribution DOUBLE,
                                          min_threshold_margin DOUBLE,
                                          ambiguous_tie BOOLEAN,
                                          high_uncertainty BOOLEAN
                                        ),
                                        edges STRUCT(
                                          prev_cluster_id BIGINT,
                                          curr_cluster_id BIGINT,
                                          edge_role VARCHAR,
                                          event_type VARCHAR,
                                          rule_basis VARCHAR,
                                          distance DOUBLE,
                                          attributed_flow DOUBLE
                                        )[],
                                        events STRUCT(
                                          event_index BIGINT,
                                          event_type VARCHAR,
                                          prev_cluster_ids BIGINT[],
                                          curr_cluster_ids BIGINT[],
                                          rule_basis VARCHAR,
                                          net_delta_fraction DOUBLE,
                                          attributed_flow_sum DOUBLE
                                        )[]
                                      ),
        PRIMARY KEY (evolution_run_id, prev_ordinal),
        UNIQUE (evolution_run_id, curr_ordinal),
        CHECK (curr_ordinal = prev_ordinal + 1),
        CHECK ((transition_status = 'gap') = chain_broken),
        CHECK ((transition_status = 'ok') = (evolve_result IS NOT NULL))
    );

    CREATE TABLE IF NOT EXISTS evolution_edges (
        evolution_run_id           VARCHAR NOT NULL,
        prev_ordinal               INTEGER NOT NULL,
        prev_cluster_id            INTEGER NOT NULL,
        curr_ordinal               INTEGER NOT NULL,
        curr_cluster_id            INTEGER NOT NULL,
        prev_branch_id             VARCHAR NOT NULL,
        curr_branch_id             VARCHAR NOT NULL,
        edge_role                  VARCHAR NOT NULL CHECK (edge_role IN ('primary', 'secondary_split', 'secondary_merge')),
        event_type                 VARCHAR NOT NULL CHECK (event_type IN ('continuation', 'split', 'merge')),
        rule_basis                 VARCHAR NOT NULL,
        distance                   DOUBLE NOT NULL CHECK (isfinite(distance) AND distance >= 0),
        distance_over_scale        DOUBLE NOT NULL CHECK (isfinite(distance_over_scale) AND distance_over_scale >= 0),
        prev_member_fraction       DOUBLE NOT NULL CHECK (isfinite(prev_member_fraction) AND prev_member_fraction > 0),
        curr_member_fraction       DOUBLE NOT NULL CHECK (isfinite(curr_member_fraction) AND curr_member_fraction > 0),
        edge_delta_fraction        DOUBLE NOT NULL CHECK (isfinite(edge_delta_fraction)),
        attributed_flow            DOUBLE NOT NULL CHECK (isfinite(attributed_flow) AND attributed_flow > 0),
        edge_strength              DOUBLE NOT NULL CHECK (isfinite(edge_strength) AND edge_strength >= 0 AND edge_strength <= 1),
        threshold_margin           DOUBLE NOT NULL CHECK (isfinite(threshold_margin) AND threshold_margin >= 0),
        threshold_margin_ratio     DOUBLE NOT NULL CHECK (isfinite(threshold_margin_ratio) AND threshold_margin_ratio >= 0),
        primary_alternative_status VARCHAR NOT NULL,
        has_optimality_margin      BOOLEAN NOT NULL,
        optimality_margin          DOUBLE NOT NULL CHECK (isfinite(optimality_margin) AND optimality_margin >= 0),
        optimality_margin_ratio    DOUBLE NOT NULL CHECK (isfinite(optimality_margin_ratio) AND optimality_margin_ratio >= 0),
        has_secondary_margin       BOOLEAN NOT NULL,
        secondary_margin           DOUBLE NOT NULL CHECK (isfinite(secondary_margin) AND secondary_margin >= 0),
        secondary_margin_ratio     DOUBLE NOT NULL CHECK (isfinite(secondary_margin_ratio) AND secondary_margin_ratio >= 0),
        ambiguous_tie              BOOLEAN NOT NULL,
        high_uncertainty           BOOLEAN NOT NULL,
        uncertainty_reasons        VARCHAR[] NOT NULL,
        non_physical_disclaimer    VARCHAR NOT NULL CHECK (length(non_physical_disclaimer) > 0),
        PRIMARY KEY (evolution_run_id, prev_ordinal, prev_cluster_id, curr_cluster_id),
        CHECK (curr_ordinal = prev_ordinal + 1),
        FOREIGN KEY (evolution_run_id, prev_ordinal) REFERENCES evolution_transitions(evolution_run_id, prev_ordinal),
        FOREIGN KEY (evolution_run_id, prev_ordinal, prev_cluster_id) REFERENCES evolution_nodes(evolution_run_id, ordinal, cluster_id),
        FOREIGN KEY (evolution_run_id, curr_ordinal, curr_cluster_id) REFERENCES evolution_nodes(evolution_run_id, ordinal, cluster_id)
    );

    CREATE TABLE IF NOT EXISTS evolution_events (
        evolution_run_id          VARCHAR NOT NULL,
        prev_ordinal              INTEGER NOT NULL,
        event_index               INTEGER NOT NULL CHECK (event_index >= 0),
        event_type                VARCHAR NOT NULL CHECK (event_type IN (
                                    'continuation', 'split', 'merge',
                                    'emergence', 'disappearance',
                                    'gap_interrupted', 'gap_restarted')),
        prev_cluster_ids          INTEGER[] NOT NULL,
        curr_cluster_ids          INTEGER[] NOT NULL,
        branch_ids                VARCHAR[] NOT NULL,
        primary_branch_id         VARCHAR,
        rule_basis                VARCHAR NOT NULL,
        max_morphology_distance   DOUBLE NOT NULL CHECK (isfinite(max_morphology_distance) AND max_morphology_distance >= 0),
        max_morphology_over_scale DOUBLE NOT NULL CHECK (isfinite(max_morphology_over_scale) AND max_morphology_over_scale >= 0),
        prev_fraction_sum         DOUBLE NOT NULL CHECK (isfinite(prev_fraction_sum) AND prev_fraction_sum >= 0),
        curr_fraction_sum         DOUBLE NOT NULL CHECK (isfinite(curr_fraction_sum) AND curr_fraction_sum >= 0),
        net_delta_fraction        DOUBLE NOT NULL CHECK (isfinite(net_delta_fraction)),
        attributed_flow_sum       DOUBLE NOT NULL CHECK (isfinite(attributed_flow_sum) AND attributed_flow_sum >= 0),
        has_min_margin_ratio      BOOLEAN NOT NULL,
        min_margin_ratio          DOUBLE NOT NULL CHECK (isfinite(min_margin_ratio) AND min_margin_ratio >= 0),
        ambiguous_tie             BOOLEAN NOT NULL,
        high_uncertainty          BOOLEAN NOT NULL,
        non_physical_disclaimer   VARCHAR NOT NULL CHECK (length(non_physical_disclaimer) > 0),
        PRIMARY KEY (evolution_run_id, prev_ordinal, event_index),
        FOREIGN KEY (evolution_run_id, prev_ordinal) REFERENCES evolution_transitions(evolution_run_id, prev_ordinal)
    );

    CREATE TABLE IF NOT EXISTS evolution_branch_summaries (
        evolution_run_id                 VARCHAR NOT NULL REFERENCES evolution_runs(evolution_run_id),
        branch_id                        VARCHAR NOT NULL,
        birth_ordinal                    INTEGER NOT NULL CHECK (birth_ordinal >= 0),
        end_ordinal                      INTEGER NOT NULL CHECK (end_ordinal >= birth_ordinal),
        node_count                       INTEGER NOT NULL CHECK (node_count >= 1),
        step_count                       INTEGER NOT NULL CHECK (step_count = node_count - 1),
        birth_reason                     VARCHAR NOT NULL,
        termination_reason               VARCHAR NOT NULL,
        cumulative_morphology_distance   DOUBLE NOT NULL CHECK (isfinite(cumulative_morphology_distance) AND cumulative_morphology_distance >= 0),
        cumulative_morphology_over_scale DOUBLE NOT NULL CHECK (isfinite(cumulative_morphology_over_scale) AND cumulative_morphology_over_scale >= 0),
        max_step_morphology_distance     DOUBLE NOT NULL CHECK (isfinite(max_step_morphology_distance) AND max_step_morphology_distance >= 0),
        max_step_morphology_over_scale   DOUBLE NOT NULL CHECK (isfinite(max_step_morphology_over_scale) AND max_step_morphology_over_scale >= 0),
        end_to_end_morphology_distance   DOUBLE NOT NULL CHECK (isfinite(end_to_end_morphology_distance) AND end_to_end_morphology_distance >= 0),
        initial_member_fraction          DOUBLE NOT NULL CHECK (isfinite(initial_member_fraction) AND initial_member_fraction > 0),
        final_member_fraction            DOUBLE NOT NULL CHECK (isfinite(final_member_fraction) AND final_member_fraction > 0),
        min_member_fraction              DOUBLE NOT NULL CHECK (isfinite(min_member_fraction) AND min_member_fraction > 0),
        max_member_fraction              DOUBLE NOT NULL CHECK (isfinite(max_member_fraction) AND max_member_fraction <= 1),
        net_delta_fraction               DOUBLE NOT NULL CHECK (isfinite(net_delta_fraction)),
        cumulative_abs_delta_fraction    DOUBLE NOT NULL CHECK (isfinite(cumulative_abs_delta_fraction) AND cumulative_abs_delta_fraction >= 0),
        split_out_event_count            INTEGER NOT NULL CHECK (split_out_event_count >= 0),
        spawned_secondary_branch_ids     VARCHAR[] NOT NULL,
        split_from_parent_branch_id      VARCHAR,
        merge_in_event_count             INTEGER NOT NULL CHECK (merge_in_event_count >= 0),
        absorbed_secondary_branch_ids    VARCHAR[] NOT NULL,
        merged_into_branch_id            VARCHAR,
        ambiguous_step_count             INTEGER NOT NULL CHECK (ambiguous_step_count >= 0),
        high_uncertainty_step_count      INTEGER NOT NULL CHECK (high_uncertainty_step_count >= 0),
        high_uncertainty_step_ratio      DOUBLE NOT NULL CHECK (isfinite(high_uncertainty_step_ratio) AND high_uncertainty_step_ratio >= 0 AND high_uncertainty_step_ratio <= 1),
        PRIMARY KEY (evolution_run_id, branch_id)
    );

    BEGIN TRANSACTION;

    INSERT INTO tracking_runs VALUES (
        '{tracking_run_key}',
        {{
            'run_id': 'm2_showcase_run_001',
            'input_manifest_sha256': '1111111111111111111111111111111111111111111111111111111111111111',
            'scenario_config_sha256': '2222222222222222222222222222222222222222222222222222222222222222',
            'match_config_sha256': '3333333333333333333333333333333333333333333333333333333333333333',
            'weights_version': 'cos_lat_center_v1',
            'threshold_coefficient': 1.0::DOUBLE,
            'cluster_count': 3::INTEGER,
            'software': {{
                'ensabc_extension_sha256': '4444444444444444444444444444444444444444444444444444444444444444',
                'duckomo_version': 'v0.3.5-rc.1',
                'duckomo_sha256': '5555555555555555555555555555555555555555555555555555555555555555',
                'duckdb_version': 'v1.5.5',
                'cluster_algorithm_version': 'm1.native.v1',
                'match_algorithm_version': 'm2.match.v1'
            }}
        }},
        [0.5, 0.5]::DOUBLE[],
        8,
        '6666666666666666666666666666666666666666666666666666666666666666'
    );

    INSERT INTO replay_batches VALUES
        ('{tracking_run_key}', 0, TIMESTAMP '2020-01-01 00:00:00', 96, 'ok', NULL, 'in0', 'om0', 1.0::DOUBLE, 'cr0'),
        ('{tracking_run_key}', 1, TIMESTAMP '2020-01-01 12:00:00', 84, 'ok', NULL, 'in1', 'om1', 1.0::DOUBLE, 'cr1'),
        ('{tracking_run_key}', 2, TIMESTAMP '2020-01-02 00:00:00', 72, 'ok', NULL, 'in2', 'om2', 1.0::DOUBLE, 'cr2'),
        ('{tracking_run_key}', 3, TIMESTAMP '2020-01-02 12:00:00', 60, 'missing', 'MISSING_UPSTREAM_FILE', NULL, NULL, NULL, NULL),
        ('{tracking_run_key}', 4, TIMESTAMP '2020-01-03 00:00:00', 48, 'ok', NULL, 'in4', 'om4', 1.0::DOUBLE, 'cr4'),
        ('{tracking_run_key}', 5, TIMESTAMP '2020-01-03 12:00:00', 36, 'ok', NULL, 'in5', 'om5', 1.0::DOUBLE, 'cr5'),
        ('{tracking_run_key}', 6, TIMESTAMP '2020-01-04 00:00:00', 24, 'failed', 'EMPTY_CLUSTER', 'in6', 'om6', NULL, NULL),
        ('{tracking_run_key}', 7, TIMESTAMP '2020-01-04 12:00:00', 12, 'ok', NULL, 'in7', 'om7', 1.0::DOUBLE, 'cr7');

    INSERT INTO scenario_nodes VALUES
        ('{tracking_run_key}', 0, 0, 0, [270.0, 270.0]::DOUBLE[], 20, 0.40::DOUBLE, 'm0', 0.1::DOUBLE, '{pfx}:b00:k00', 'first_batch'),
        ('{tracking_run_key}', 0, 1, 1, [280.0, 280.0]::DOUBLE[], 20, 0.40::DOUBLE, 'm1', 0.1::DOUBLE, '{pfx}:b00:k01', 'first_batch'),
        ('{tracking_run_key}', 0, 2, 2, [290.0, 290.0]::DOUBLE[], 10, 0.20::DOUBLE, 'm2', 0.1::DOUBLE, '{pfx}:b00:k02', 'first_batch'),
        ('{tracking_run_key}', 1, 0, 0, [270.25, 270.25]::DOUBLE[], 15, 0.30::DOUBLE, 'm0', 0.1::DOUBLE, '{pfx}:b00:k00', 'inherited'),
        ('{tracking_run_key}', 1, 1, 1, [280.20, 280.20]::DOUBLE[], 15, 0.30::DOUBLE, 'm1', 0.1::DOUBLE, '{pfx}:b00:k01', 'inherited'),
        ('{tracking_run_key}', 1, 2, 2, [280.50, 280.50]::DOUBLE[], 10, 0.20::DOUBLE, 'm2', 0.1::DOUBLE, '{pfx}:b01:k02', 'not_selected_in_global_optimum'),
        ('{tracking_run_key}', 1, 3, 3, [295.0, 295.0]::DOUBLE[], 10, 0.20::DOUBLE, 'm3', 0.1::DOUBLE, '{pfx}:b01:k03', 'no_candidate_within_threshold'),
        ('{tracking_run_key}', 2, 0, 0, [270.30, 270.30]::DOUBLE[], 15, 0.30::DOUBLE, 'm0', 0.1::DOUBLE, '{pfx}:b00:k00', 'inherited'),
        ('{tracking_run_key}', 2, 1, 1, [280.35, 280.35]::DOUBLE[], 25, 0.50::DOUBLE, 'm1', 0.1::DOUBLE, '{pfx}:b00:k01', 'inherited_ambiguous'),
        ('{tracking_run_key}', 2, 2, 2, [295.95, 295.95]::DOUBLE[], 10, 0.20::DOUBLE, 'm2', 0.1::DOUBLE, '{pfx}:b01:k03', 'inherited'),
        ('{tracking_run_key}', 4, 0, 0, [272.0, 272.0]::DOUBLE[], 30, 0.60::DOUBLE, 'm0', 0.1::DOUBLE, '{pfx}:b04:k00', 'previous_planned_batch_unavailable'),
        ('{tracking_run_key}', 4, 1, 1, [288.0, 288.0]::DOUBLE[], 20, 0.40::DOUBLE, 'm1', 0.1::DOUBLE, '{pfx}:b04:k01', 'previous_planned_batch_unavailable'),
        ('{tracking_run_key}', 5, 0, 0, [272.20, 272.20]::DOUBLE[], 28, 0.56::DOUBLE, 'm0', 0.1::DOUBLE, '{pfx}:b04:k00', 'inherited'),
        ('{tracking_run_key}', 5, 1, 1, [288.25, 288.25]::DOUBLE[], 22, 0.44::DOUBLE, 'm1', 0.1::DOUBLE, '{pfx}:b04:k01', 'inherited'),
        ('{tracking_run_key}', 7, 0, 0, [273.0, 273.0]::DOUBLE[], 25, 0.50::DOUBLE, 'm0', 0.1::DOUBLE, '{pfx}:b07:k00', 'previous_planned_batch_unavailable'),
        ('{tracking_run_key}', 7, 1, 1, [289.0, 289.0]::DOUBLE[], 25, 0.50::DOUBLE, 'm1', 0.1::DOUBLE, '{pfx}:b07:k01', 'previous_planned_batch_unavailable');

    INSERT INTO pair_comparisons
    SELECT
        '{tracking_run_key}',
        prev_ord,
        prev_ord + 1,
        1.0::DOUBLE,
        1.0::DOUBLE,
        {{
            'algorithm_version': 'm2.match.v1',
            'prev_cluster_ids': [0::BIGINT, 1::BIGINT],
            'curr_cluster_ids': [0::BIGINT, 1::BIGINT],
            'prev_dispersion': 1.0::DOUBLE,
            'curr_dispersion': 1.0::DOUBLE,
            'pair_scale': 1.0::DOUBLE,
            'threshold_coefficient': 1.0::DOUBLE,
            'threshold': 1.0::DOUBLE,
            'distance_matrix': [[0.2::DOUBLE]]::DOUBLE[][],
            'distance_ratio_matrix': [[0.2::DOUBLE]]::DOUBLE[][],
            'eligible_matrix': [[true]]::BOOLEAN[][],
            'edge_count': 2::BIGINT,
            'total_distance': 0.45::DOUBLE,
            'optimal_solution_count': 1::BIGINT,
            'ambiguous': false,
            'threshold_margin': 0.5::DOUBLE,
            'optimality_margin': 0.3::DOUBLE,
            'has_optimality_margin': true,
            'links': []::STRUCT(prev_cluster_id BIGINT, curr_cluster_id BIGINT, distance DOUBLE, distance_ratio DOUBLE, alternative_status VARCHAR, alternative_edge_count BIGINT, alternative_total_distance DOUBLE, alternative_distance_increase DOUBLE, alternative_distance_increase_ratio DOUBLE, has_same_count_alternative BOOLEAN, ambiguous BOOLEAN)[],
            'unmatched_prev': []::STRUCT(cluster_id BIGINT, reason VARCHAR, min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN)[],
            'unmatched_curr': []::STRUCT(cluster_id BIGINT, reason VARCHAR, min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN)[]
        }}
    FROM (VALUES (0), (1), (4)) t(prev_ord);

    INSERT INTO scenario_links VALUES
        ('{tracking_run_key}', 0, 0, 1, 0, 0.25::DOUBLE, 0.25::DOUBLE, 'count_drop', false),
        ('{tracking_run_key}', 0, 1, 1, 1, 0.20::DOUBLE, 0.20::DOUBLE, 'same_count', false),
        ('{tracking_run_key}', 1, 0, 2, 0, 0.05::DOUBLE, 0.05::DOUBLE, 'count_drop', false),
        ('{tracking_run_key}', 1, 1, 2, 1, 0.15::DOUBLE, 0.15::DOUBLE, 'tied', true),
        ('{tracking_run_key}', 1, 3, 2, 2, 0.95::DOUBLE, 0.95::DOUBLE, 'count_drop', false),
        ('{tracking_run_key}', 4, 0, 5, 0, 0.20::DOUBLE, 0.20::DOUBLE, 'count_drop', false),
        ('{tracking_run_key}', 4, 1, 5, 1, 0.25::DOUBLE, 0.25::DOUBLE, 'count_drop', false);

    INSERT INTO evolution_runs VALUES (
        '{evolution_run_id}',
        '{tracking_run_key}',
        {{
            'tracking_run_key': '{tracking_run_key}',
            'input_manifest_sha256': '1111111111111111111111111111111111111111111111111111111111111111',
            'scenario_config_sha256': '2222222222222222222222222222222222222222222222222222222222222222',
            'match_config_sha256': '3333333333333333333333333333333333333333333333333333333333333333',
            'evolve_config_sha256': '7777777777777777777777777777777777777777777777777777777777777777',
            'weights_version': 'cos_lat_center_v1',
            'secondary_rule': 'nearest_feasible_v1',
            'flow_rule': 'star_min_mass_proportional_v1',
            'uncertainty_rule': 'm3.uncertainty.v1',
            'fraction_sum_tolerance': 1e-9::DOUBLE,
            'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
            'high_uncertainty_optimality_ratio': 0.10::DOUBLE,
            'high_uncertainty_secondary_ratio': 0.10::DOUBLE,
            'software': {{
                'ensabc_extension_sha256': '4444444444444444444444444444444444444444444444444444444444444444',
                'duckomo_version': 'v0.3.5-rc.1',
                'duckomo_sha256': '5555555555555555555555555555555555555555555555555555555555555555',
                'duckdb_version': 'v1.5.5',
                'cluster_algorithm_version': 'm1.native.v1',
                'match_algorithm_version': 'm2.match.v1',
                'evolution_algorithm_version': 'm3.evolve.v1'
            }}
        }},
        [0.5, 0.5]::DOUBLE[],
        8,
        {{
            'ok_batch_count': 6,
            'missing_batch_count': 1,
            'failed_batch_count': 1,
            'ok_transition_count': 3,
            'gap_transition_count': 4,
            'branch_count': 9,
            'primary_edge_count': 7,
            'secondary_split_edge_count': 1,
            'secondary_merge_edge_count': 1,
            'continuation_count': 5,
            'split_count': 1,
            'merge_count': 1,
            'emergence_count': 1,
            'disappearance_count': 1,
            'gap_interrupted_count': 5,
            'gap_restarted_count': 4,
            'cumulative_primary_distance': 2.05::DOUBLE,
            'max_edge_distance': 0.95::DOUBLE,
            'max_edge_distance_over_scale': 0.95::DOUBLE,
            'mean_transition_tv_redistribution': 0.14666666666666667::DOUBLE,
            'max_transition_tv_redistribution': 0.20::DOUBLE,
            'ambiguous_transition_count': 1,
            'high_uncertainty_transition_count': 1,
            'high_uncertainty_transition_ratio': 0.3333333333333333::DOUBLE,
            'high_uncertainty_edge_count': 3,
            'high_uncertainty_edge_ratio': 0.3333333333333333::DOUBLE
        }},
        '{NON_PHYSICAL_TOKEN}',
        '8888888888888888888888888888888888888888888888888888888888888888'
    );

    INSERT INTO evolution_nodes VALUES
        ('{evolution_run_id}', 0, 0, 0, '{pfx}:b00:k00', [270.0, 270.0]::DOUBLE[], 20, 0.40::DOUBLE, 'first_batch', 0, 0, 1, 0, 'first_batch', 'continuation_source', 0.0::DOUBLE, false, 0.0::DOUBLE, 0.10::DOUBLE, false),
        ('{evolution_run_id}', 0, 1, 1, '{pfx}:b00:k01', [280.0, 280.0]::DOUBLE[], 20, 0.40::DOUBLE, 'first_batch', 0, 0, 1, 1, 'first_batch', 'split_parent', 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false),
        ('{evolution_run_id}', 0, 2, 2, '{pfx}:b00:k02', [290.0, 290.0]::DOUBLE[], 10, 0.20::DOUBLE, 'first_batch', 0, 0, 0, 0, 'first_batch', 'disappeared', 0.0::DOUBLE, false, 0.0::DOUBLE, 0.20::DOUBLE, false),
        ('{evolution_run_id}', 1, 0, 0, '{pfx}:b00:k00', [270.25, 270.25]::DOUBLE[], 15, 0.30::DOUBLE, 'inherited', 1, 0, 1, 0, 'continuation_target', 'continuation_source', -0.10::DOUBLE, true, 0.0::DOUBLE, 0.0::DOUBLE, false),
        ('{evolution_run_id}', 1, 1, 1, '{pfx}:b00:k01', [280.20, 280.20]::DOUBLE[], 15, 0.30::DOUBLE, 'inherited', 1, 0, 1, 0, 'split_primary_child', 'merge_primary_parent', -0.10::DOUBLE, true, 0.06::DOUBLE, 0.0::DOUBLE, true),
        ('{evolution_run_id}', 1, 2, 2, '{pfx}:b01:k02', [280.50, 280.50]::DOUBLE[], 10, 0.20::DOUBLE, 'not_selected_in_global_optimum', 0, 1, 0, 1, 'split_secondary_child', 'merge_secondary_parent', 0.0::DOUBLE, false, 0.04::DOUBLE, 0.0::DOUBLE, true),
        ('{evolution_run_id}', 1, 3, 3, '{pfx}:b01:k03', [295.0, 295.0]::DOUBLE[], 10, 0.20::DOUBLE, 'no_candidate_within_threshold', 0, 0, 1, 0, 'emerged', 'continuation_source', 0.0::DOUBLE, false, 0.20::DOUBLE, 0.0::DOUBLE, true),
        ('{evolution_run_id}', 2, 0, 0, '{pfx}:b00:k00', [270.30, 270.30]::DOUBLE[], 15, 0.30::DOUBLE, 'inherited', 1, 0, 0, 0, 'continuation_target', 'gap_interrupted', 0.0::DOUBLE, true, 0.0::DOUBLE, 0.30::DOUBLE, false),
        ('{evolution_run_id}', 2, 1, 1, '{pfx}:b00:k01', [280.35, 280.35]::DOUBLE[], 25, 0.50::DOUBLE, 'inherited_ambiguous', 1, 1, 0, 0, 'merge_target', 'gap_interrupted', 0.20::DOUBLE, true, 0.0::DOUBLE, 0.50::DOUBLE, true),
        ('{evolution_run_id}', 2, 2, 2, '{pfx}:b01:k03', [295.95, 295.95]::DOUBLE[], 10, 0.20::DOUBLE, 'inherited', 1, 0, 0, 0, 'continuation_target', 'gap_interrupted', 0.0::DOUBLE, true, 0.0::DOUBLE, 0.20::DOUBLE, true),
        ('{evolution_run_id}', 4, 0, 0, '{pfx}:b04:k00', [272.0, 272.0]::DOUBLE[], 30, 0.60::DOUBLE, 'previous_planned_batch_unavailable', 0, 0, 1, 0, 'gap_restarted', 'continuation_source', 0.0::DOUBLE, false, 0.60::DOUBLE, 0.04::DOUBLE, false),
        ('{evolution_run_id}', 4, 1, 1, '{pfx}:b04:k01', [288.0, 288.0]::DOUBLE[], 20, 0.40::DOUBLE, 'previous_planned_batch_unavailable', 0, 0, 1, 0, 'gap_restarted', 'continuation_source', 0.0::DOUBLE, false, 0.40::DOUBLE, 0.0::DOUBLE, false),
        ('{evolution_run_id}', 5, 0, 0, '{pfx}:b04:k00', [272.20, 272.20]::DOUBLE[], 28, 0.56::DOUBLE, 'inherited', 1, 0, 0, 0, 'continuation_target', 'gap_interrupted', -0.04::DOUBLE, true, 0.0::DOUBLE, 0.56::DOUBLE, false),
        ('{evolution_run_id}', 5, 1, 1, '{pfx}:b04:k01', [288.25, 288.25]::DOUBLE[], 22, 0.44::DOUBLE, 'inherited', 1, 0, 0, 0, 'continuation_target', 'gap_interrupted', 0.04::DOUBLE, true, 0.04::DOUBLE, 0.44::DOUBLE, false),
        ('{evolution_run_id}', 7, 0, 0, '{pfx}:b07:k00', [273.0, 273.0]::DOUBLE[], 25, 0.50::DOUBLE, 'previous_planned_batch_unavailable', 0, 0, 0, 0, 'gap_restarted', 'final_batch', 0.0::DOUBLE, false, 0.50::DOUBLE, 0.0::DOUBLE, false),
        ('{evolution_run_id}', 7, 1, 1, '{pfx}:b07:k01', [289.0, 289.0]::DOUBLE[], 25, 0.50::DOUBLE, 'previous_planned_batch_unavailable', 0, 0, 0, 0, 'gap_restarted', 'final_batch', 0.0::DOUBLE, false, 0.50::DOUBLE, 0.0::DOUBLE, false);

    INSERT INTO evolution_transitions VALUES
        ('{evolution_run_id}', 0, 1, 'ok', false, NULL, 1.0::DOUBLE, 1.0::DOUBLE, 2, 1, 0, 0.20::DOUBLE, 0.50::DOUBLE, false, false, {{
            'algorithm_version': 'm3.evolve.v1',
            'summary': {{
                'primary_edge_count': 2::BIGINT,
                'secondary_split_edge_count': 1::BIGINT,
                'secondary_merge_edge_count': 0::BIGINT,
                'transition_tv_redistribution': 0.20::DOUBLE,
                'min_threshold_margin': 0.50::DOUBLE,
                'ambiguous_tie': false,
                'high_uncertainty': false
            }},
            'edges': [
                {{'prev_cluster_id': 0::BIGINT, 'curr_cluster_id': 0::BIGINT, 'edge_role': 'primary', 'event_type': 'continuation', 'rule_basis': 'global_optimum_primary', 'distance': 0.25::DOUBLE, 'attributed_flow': 0.30::DOUBLE}},
                {{'prev_cluster_id': 1::BIGINT, 'curr_cluster_id': 1::BIGINT, 'edge_role': 'primary', 'event_type': 'split', 'rule_basis': 'global_optimum_primary', 'distance': 0.20::DOUBLE, 'attributed_flow': 0.24::DOUBLE}},
                {{'prev_cluster_id': 1::BIGINT, 'curr_cluster_id': 2::BIGINT, 'edge_role': 'secondary_split', 'event_type': 'split', 'rule_basis': 'nearest_feasible_split_unique', 'distance': 0.50::DOUBLE, 'attributed_flow': 0.16::DOUBLE}}
            ],
            'events': [
                {{'event_index': 0::BIGINT, 'event_type': 'continuation', 'prev_cluster_ids': [0::BIGINT], 'curr_cluster_ids': [0::BIGINT], 'rule_basis': 'single_primary_continuation', 'net_delta_fraction': -0.10::DOUBLE, 'attributed_flow_sum': 0.30::DOUBLE}},
                {{'event_index': 1::BIGINT, 'event_type': 'split', 'prev_cluster_ids': [1::BIGINT], 'curr_cluster_ids': [1::BIGINT, 2::BIGINT], 'rule_basis': 'primary_plus_nearest_feasible_split', 'net_delta_fraction': 0.10::DOUBLE, 'attributed_flow_sum': 0.40::DOUBLE}},
                {{'event_index': 2::BIGINT, 'event_type': 'disappearance', 'prev_cluster_ids': [2::BIGINT], 'curr_cluster_ids': []::BIGINT[], 'rule_basis': 'no_candidate_within_threshold', 'net_delta_fraction': -0.20::DOUBLE, 'attributed_flow_sum': 0.0::DOUBLE}},
                {{'event_index': 3::BIGINT, 'event_type': 'emergence', 'prev_cluster_ids': []::BIGINT[], 'curr_cluster_ids': [3::BIGINT], 'rule_basis': 'no_candidate_within_threshold', 'net_delta_fraction': 0.20::DOUBLE, 'attributed_flow_sum': 0.0::DOUBLE}}
            ]
        }}),
        ('{evolution_run_id}', 1, 2, 'ok', false, NULL, 1.0::DOUBLE, 1.0::DOUBLE, 3, 0, 1, 0.20::DOUBLE, 0.05::DOUBLE, true, true, {{
            'algorithm_version': 'm3.evolve.v1',
            'summary': {{
                'primary_edge_count': 3::BIGINT,
                'secondary_split_edge_count': 0::BIGINT,
                'secondary_merge_edge_count': 1::BIGINT,
                'transition_tv_redistribution': 0.20::DOUBLE,
                'min_threshold_margin': 0.05::DOUBLE,
                'ambiguous_tie': true,
                'high_uncertainty': true
            }},
            'edges': [
                {{'prev_cluster_id': 0::BIGINT, 'curr_cluster_id': 0::BIGINT, 'edge_role': 'primary', 'event_type': 'continuation', 'rule_basis': 'global_optimum_primary', 'distance': 0.05::DOUBLE, 'attributed_flow': 0.30::DOUBLE}},
                {{'prev_cluster_id': 1::BIGINT, 'curr_cluster_id': 1::BIGINT, 'edge_role': 'primary', 'event_type': 'merge', 'rule_basis': 'global_optimum_primary_canonical_tiebreak', 'distance': 0.15::DOUBLE, 'attributed_flow': 0.30::DOUBLE}},
                {{'prev_cluster_id': 2::BIGINT, 'curr_cluster_id': 1::BIGINT, 'edge_role': 'secondary_merge', 'event_type': 'merge', 'rule_basis': 'nearest_feasible_merge_unique', 'distance': 0.15::DOUBLE, 'attributed_flow': 0.20::DOUBLE}},
                {{'prev_cluster_id': 3::BIGINT, 'curr_cluster_id': 2::BIGINT, 'edge_role': 'primary', 'event_type': 'continuation', 'rule_basis': 'global_optimum_primary', 'distance': 0.95::DOUBLE, 'attributed_flow': 0.20::DOUBLE}}
            ],
            'events': [
                {{'event_index': 0::BIGINT, 'event_type': 'continuation', 'prev_cluster_ids': [0::BIGINT], 'curr_cluster_ids': [0::BIGINT], 'rule_basis': 'single_primary_continuation', 'net_delta_fraction': 0.0::DOUBLE, 'attributed_flow_sum': 0.30::DOUBLE}},
                {{'event_index': 1::BIGINT, 'event_type': 'merge', 'prev_cluster_ids': [1::BIGINT, 2::BIGINT], 'curr_cluster_ids': [1::BIGINT], 'rule_basis': 'primary_plus_nearest_feasible_merge', 'net_delta_fraction': 0.0::DOUBLE, 'attributed_flow_sum': 0.50::DOUBLE}},
                {{'event_index': 2::BIGINT, 'event_type': 'continuation', 'prev_cluster_ids': [3::BIGINT], 'curr_cluster_ids': [2::BIGINT], 'rule_basis': 'single_primary_continuation', 'net_delta_fraction': 0.0::DOUBLE, 'attributed_flow_sum': 0.20::DOUBLE}}
            ]
        }}),
        ('{evolution_run_id}', 2, 3, 'gap', true, 'curr_unavailable', NULL, NULL, 0, 0, 0, NULL, NULL, false, false, NULL),
        ('{evolution_run_id}', 3, 4, 'gap', true, 'prev_unavailable', NULL, NULL, 0, 0, 0, NULL, NULL, false, false, NULL),
        ('{evolution_run_id}', 4, 5, 'ok', false, NULL, 1.0::DOUBLE, 1.0::DOUBLE, 2, 0, 0, 0.04::DOUBLE, 0.75::DOUBLE, false, false, {{
            'algorithm_version': 'm3.evolve.v1',
            'summary': {{
                'primary_edge_count': 2::BIGINT,
                'secondary_split_edge_count': 0::BIGINT,
                'secondary_merge_edge_count': 0::BIGINT,
                'transition_tv_redistribution': 0.04::DOUBLE,
                'min_threshold_margin': 0.75::DOUBLE,
                'ambiguous_tie': false,
                'high_uncertainty': false
            }},
            'edges': [
                {{'prev_cluster_id': 0::BIGINT, 'curr_cluster_id': 0::BIGINT, 'edge_role': 'primary', 'event_type': 'continuation', 'rule_basis': 'global_optimum_primary', 'distance': 0.20::DOUBLE, 'attributed_flow': 0.56::DOUBLE}},
                {{'prev_cluster_id': 1::BIGINT, 'curr_cluster_id': 1::BIGINT, 'edge_role': 'primary', 'event_type': 'continuation', 'rule_basis': 'global_optimum_primary', 'distance': 0.25::DOUBLE, 'attributed_flow': 0.40::DOUBLE}}
            ],
            'events': [
                {{'event_index': 0::BIGINT, 'event_type': 'continuation', 'prev_cluster_ids': [0::BIGINT], 'curr_cluster_ids': [0::BIGINT], 'rule_basis': 'single_primary_continuation', 'net_delta_fraction': -0.04::DOUBLE, 'attributed_flow_sum': 0.56::DOUBLE}},
                {{'event_index': 1::BIGINT, 'event_type': 'continuation', 'prev_cluster_ids': [1::BIGINT], 'curr_cluster_ids': [1::BIGINT], 'rule_basis': 'single_primary_continuation', 'net_delta_fraction': 0.04::DOUBLE, 'attributed_flow_sum': 0.40::DOUBLE}}
            ]
        }}),
        ('{evolution_run_id}', 5, 6, 'gap', true, 'curr_unavailable', NULL, NULL, 0, 0, 0, NULL, NULL, false, false, NULL),
        ('{evolution_run_id}', 6, 7, 'gap', true, 'prev_unavailable', NULL, NULL, 0, 0, 0, NULL, NULL, false, false, NULL);

    INSERT INTO evolution_edges VALUES
        ('{evolution_run_id}', 0, 0, 1, 0, '{pfx}:b00:k00', '{pfx}:b00:k00', 'primary', 'continuation', 'global_optimum_primary', 0.25::DOUBLE, 0.25::DOUBLE, 0.40::DOUBLE, 0.30::DOUBLE, -0.10::DOUBLE, 0.30::DOUBLE, 0.75::DOUBLE, 0.75::DOUBLE, 0.75::DOUBLE, 'count_drop', false, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, false, []::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 0, 1, 1, 1, '{pfx}:b00:k01', '{pfx}:b00:k01', 'primary', 'split', 'global_optimum_primary', 0.20::DOUBLE, 0.20::DOUBLE, 0.40::DOUBLE, 0.30::DOUBLE, -0.10::DOUBLE, 0.24::DOUBLE, 0.80::DOUBLE, 0.80::DOUBLE, 0.80::DOUBLE, 'same_count', true, 0.30::DOUBLE, 0.30::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, false, []::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 0, 1, 1, 2, '{pfx}:b00:k01', '{pfx}:b01:k02', 'secondary_split', 'split', 'nearest_feasible_split_unique', 0.50::DOUBLE, 0.50::DOUBLE, 0.40::DOUBLE, 0.20::DOUBLE, -0.20::DOUBLE, 0.16::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 'promotion_gap', true, 0.30::DOUBLE, 0.30::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, false, []::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 0, 2, 0, '{pfx}:b00:k00', '{pfx}:b00:k00', 'primary', 'continuation', 'global_optimum_primary', 0.05::DOUBLE, 0.05::DOUBLE, 0.30::DOUBLE, 0.30::DOUBLE, 0.0::DOUBLE, 0.30::DOUBLE, 0.95::DOUBLE, 0.95::DOUBLE, 0.95::DOUBLE, 'count_drop', false, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, false, []::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 1, 2, 1, '{pfx}:b00:k01', '{pfx}:b00:k01', 'primary', 'merge', 'global_optimum_primary_canonical_tiebreak', 0.15::DOUBLE, 0.15::DOUBLE, 0.30::DOUBLE, 0.50::DOUBLE, 0.20::DOUBLE, 0.30::DOUBLE, 0.85::DOUBLE, 0.85::DOUBLE, 0.85::DOUBLE, 'tied', true, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, true, true, ['ambiguous_tie', 'near_primary_switch']::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 2, 2, 1, '{pfx}:b01:k02', '{pfx}:b00:k01', 'secondary_merge', 'merge', 'nearest_feasible_merge_unique', 0.15::DOUBLE, 0.15::DOUBLE, 0.20::DOUBLE, 0.50::DOUBLE, 0.30::DOUBLE, 0.20::DOUBLE, 0.85::DOUBLE, 0.85::DOUBLE, 0.85::DOUBLE, 'promotion_gap', true, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, true, true, ['ambiguous_tie', 'near_primary_switch']::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 3, 2, 2, '{pfx}:b01:k03', '{pfx}:b01:k03', 'primary', 'continuation', 'global_optimum_primary', 0.95::DOUBLE, 0.95::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, 0.05::DOUBLE, 0.05::DOUBLE, 0.05::DOUBLE, 'count_drop', false, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, true, ['near_threshold']::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 4, 0, 5, 0, '{pfx}:b04:k00', '{pfx}:b04:k00', 'primary', 'continuation', 'global_optimum_primary', 0.20::DOUBLE, 0.20::DOUBLE, 0.60::DOUBLE, 0.56::DOUBLE, -0.04::DOUBLE, 0.56::DOUBLE, 0.80::DOUBLE, 0.80::DOUBLE, 0.80::DOUBLE, 'count_drop', false, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, false, []::VARCHAR[], '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 4, 1, 5, 1, '{pfx}:b04:k01', '{pfx}:b04:k01', 'primary', 'continuation', 'global_optimum_primary', 0.25::DOUBLE, 0.25::DOUBLE, 0.40::DOUBLE, 0.44::DOUBLE, 0.04::DOUBLE, 0.40::DOUBLE, 0.75::DOUBLE, 0.75::DOUBLE, 0.75::DOUBLE, 'count_drop', false, 0.0::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, 0.0::DOUBLE, false, false, []::VARCHAR[], '{NON_PHYSICAL_TOKEN}');

    INSERT INTO evolution_events VALUES
        ('{evolution_run_id}', 0, 0, 'continuation', [0]::INTEGER[], [0]::INTEGER[], ['{pfx}:b00:k00']::VARCHAR[], '{pfx}:b00:k00', 'single_primary_continuation', 0.25::DOUBLE, 0.25::DOUBLE, 0.40::DOUBLE, 0.30::DOUBLE, -0.10::DOUBLE, 0.30::DOUBLE, true, 0.75::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 0, 1, 'split', [1]::INTEGER[], [1, 2]::INTEGER[], ['{pfx}:b00:k01', '{pfx}:b01:k02']::VARCHAR[], '{pfx}:b00:k01', 'primary_plus_nearest_feasible_split', 0.50::DOUBLE, 0.50::DOUBLE, 0.40::DOUBLE, 0.50::DOUBLE, 0.10::DOUBLE, 0.40::DOUBLE, true, 0.30::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 0, 2, 'disappearance', [2]::INTEGER[], []::INTEGER[], ['{pfx}:b00:k02']::VARCHAR[], NULL, 'no_candidate_within_threshold', 0.0::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, -0.20::DOUBLE, 0.0::DOUBLE, true, 4.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 0, 3, 'emergence', []::INTEGER[], [3]::INTEGER[], ['{pfx}:b01:k03']::VARCHAR[], NULL, 'no_candidate_within_threshold', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, true, 4.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 0, 'continuation', [0]::INTEGER[], [0]::INTEGER[], ['{pfx}:b00:k00']::VARCHAR[], '{pfx}:b00:k00', 'single_primary_continuation', 0.05::DOUBLE, 0.05::DOUBLE, 0.30::DOUBLE, 0.30::DOUBLE, 0.0::DOUBLE, 0.30::DOUBLE, true, 0.95::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 1, 'merge', [1, 2]::INTEGER[], [1]::INTEGER[], ['{pfx}:b00:k01', '{pfx}:b01:k02']::VARCHAR[], '{pfx}:b00:k01', 'primary_plus_nearest_feasible_merge', 0.15::DOUBLE, 0.15::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.0::DOUBLE, 0.50::DOUBLE, true, 0.0::DOUBLE, true, true, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 1, 2, 'continuation', [3]::INTEGER[], [2]::INTEGER[], ['{pfx}:b01:k03']::VARCHAR[], '{pfx}:b01:k03', 'single_primary_continuation', 0.95::DOUBLE, 0.95::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, true, 0.05::DOUBLE, false, true, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 2, 0, 'gap_interrupted', [0]::INTEGER[], []::INTEGER[], ['{pfx}:b00:k00']::VARCHAR[], NULL, 'curr_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.30::DOUBLE, 0.0::DOUBLE, -0.30::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 2, 1, 'gap_interrupted', [1]::INTEGER[], []::INTEGER[], ['{pfx}:b00:k01']::VARCHAR[], NULL, 'curr_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.50::DOUBLE, 0.0::DOUBLE, -0.50::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 2, 2, 'gap_interrupted', [2]::INTEGER[], []::INTEGER[], ['{pfx}:b01:k03']::VARCHAR[], NULL, 'curr_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, -0.20::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 3, 0, 'gap_restarted', []::INTEGER[], [0]::INTEGER[], ['{pfx}:b04:k00']::VARCHAR[], NULL, 'prev_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.60::DOUBLE, 0.60::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 3, 1, 'gap_restarted', []::INTEGER[], [1]::INTEGER[], ['{pfx}:b04:k01']::VARCHAR[], NULL, 'prev_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.40::DOUBLE, 0.40::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 4, 0, 'continuation', [0]::INTEGER[], [0]::INTEGER[], ['{pfx}:b04:k00']::VARCHAR[], '{pfx}:b04:k00', 'single_primary_continuation', 0.20::DOUBLE, 0.20::DOUBLE, 0.60::DOUBLE, 0.56::DOUBLE, -0.04::DOUBLE, 0.56::DOUBLE, true, 0.80::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 4, 1, 'continuation', [1]::INTEGER[], [1]::INTEGER[], ['{pfx}:b04:k01']::VARCHAR[], '{pfx}:b04:k01', 'single_primary_continuation', 0.25::DOUBLE, 0.25::DOUBLE, 0.40::DOUBLE, 0.44::DOUBLE, 0.04::DOUBLE, 0.40::DOUBLE, true, 0.75::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 5, 0, 'gap_interrupted', [0]::INTEGER[], []::INTEGER[], ['{pfx}:b04:k00']::VARCHAR[], NULL, 'curr_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.56::DOUBLE, 0.0::DOUBLE, -0.56::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 5, 1, 'gap_interrupted', [1]::INTEGER[], []::INTEGER[], ['{pfx}:b04:k01']::VARCHAR[], NULL, 'curr_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.44::DOUBLE, 0.0::DOUBLE, -0.44::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 6, 0, 'gap_restarted', []::INTEGER[], [0]::INTEGER[], ['{pfx}:b07:k00']::VARCHAR[], NULL, 'prev_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}'),
        ('{evolution_run_id}', 6, 1, 'gap_restarted', []::INTEGER[], [1]::INTEGER[], ['{pfx}:b07:k01']::VARCHAR[], NULL, 'prev_planned_batch_unavailable', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.0::DOUBLE, false, 0.0::DOUBLE, false, false, '{NON_PHYSICAL_TOKEN}');

    INSERT INTO evolution_branch_summaries VALUES
        ('{evolution_run_id}', '{pfx}:b00:k00', 0, 2, 3, 2, 'first_batch', 'gap_interrupted', 0.30::DOUBLE, 0.30::DOUBLE, 0.25::DOUBLE, 0.25::DOUBLE, 0.30::DOUBLE, 0.40::DOUBLE, 0.30::DOUBLE, 0.30::DOUBLE, 0.40::DOUBLE, -0.10::DOUBLE, 0.10::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 0, 0.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b00:k01', 0, 2, 3, 2, 'first_batch', 'gap_interrupted', 0.35::DOUBLE, 0.35::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.35::DOUBLE, 0.40::DOUBLE, 0.50::DOUBLE, 0.30::DOUBLE, 0.50::DOUBLE, 0.10::DOUBLE, 0.30::DOUBLE, 1, ['{pfx}:b01:k02']::VARCHAR[], NULL, 1, ['{pfx}:b01:k02']::VARCHAR[], NULL, 1, 1, 0.5::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b00:k02', 0, 0, 1, 0, 'first_batch', 'disappeared', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 0, 0.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b01:k02', 1, 1, 1, 0, 'secondary_split', 'secondary_merge', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0, []::VARCHAR[], '{pfx}:b00:k01', 0, []::VARCHAR[], '{pfx}:b00:k01', 0, 0, 0.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b01:k03', 1, 2, 2, 1, 'emerged', 'gap_interrupted', 0.95::DOUBLE, 0.95::DOUBLE, 0.95::DOUBLE, 0.95::DOUBLE, 0.95::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 1, 1.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b04:k00', 4, 5, 2, 1, 'gap_restarted', 'gap_interrupted', 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.20::DOUBLE, 0.60::DOUBLE, 0.56::DOUBLE, 0.56::DOUBLE, 0.60::DOUBLE, -0.04::DOUBLE, 0.04::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 0, 0.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b04:k01', 4, 5, 2, 1, 'gap_restarted', 'gap_interrupted', 0.25::DOUBLE, 0.25::DOUBLE, 0.25::DOUBLE, 0.25::DOUBLE, 0.25::DOUBLE, 0.40::DOUBLE, 0.44::DOUBLE, 0.40::DOUBLE, 0.44::DOUBLE, 0.04::DOUBLE, 0.04::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 0, 0.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b07:k00', 7, 7, 1, 0, 'gap_restarted', 'final_batch', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 0, 0.0::DOUBLE),
        ('{evolution_run_id}', '{pfx}:b07:k01', 7, 7, 1, 0, 'gap_restarted', 'final_batch', 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.50::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE, 0, []::VARCHAR[], NULL, 0, []::VARCHAR[], NULL, 0, 0, 0.0::DOUBLE);

    COMMIT;
    """
    proc = run_isolated_duckdb(sql, database=db_path, readonly=False)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed seeding showcase DuckDB: {proc.stderr}")
    return {"evolution_run_id": evolution_run_id, "tracking_run_key": tracking_run_key}


def _replace_provenance_json(
    html_text: str,
    mutate_fn: Callable[[dict[str, Any]], None],
    *,
    escape_html_chars: bool = True,
) -> str:
    m = re.search(
        r"(<script\s+id=\"ensabc-evolution-provenance\"\s+type=\"application/json\">\s*)(.*?)(\s*</script>)",
        html_text,
        flags=re.DOTALL,
    )
    if not m:
        raise RuntimeError("Could not locate provenance JSON script block")
    payload = json.loads(m.group(2))
    mutate_fn(payload)
    new_json = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    if escape_html_chars:
        new_json = (
            new_json.replace("&", "\\u0026")
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
        )
    return html_text[: m.start(2)] + new_json + html_text[m.end(2) :]


def tamper_01_deleted_svg_node(html_text: str, run_id: str) -> tuple[str, str]:
    """Delete one `.evo-node` group from `<svg>` while leaving JSON untouched."""
    pat = re.compile(
        r'\s*<g class="evo-node[^"]*"[^>]*data-node-id="b0:c2"[^>]*>.*?\n\s{6}</g>',
        flags=re.DOTALL,
    )
    mutated, count = pat.subn("", html_text, count=1)
    assert count == 1, "Failed to delete target SVG node b0:c2"
    return mutated, run_id


def tamper_02_injected_orphan_path(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject an untagged `<path>` orphan inside `<svg>` outside any keyed group."""
    orphan = '\n  <path d="M 12 12 L 88 88" stroke="#ef4444" stroke-width="2" />\n</svg>'
    assert "</svg>" in html_text
    return html_text.replace("</svg>", orphan, 1), run_id


def tamper_03_svg_distance_drift(html_text: str, run_id: str) -> tuple[str, str]:
    """Drift `data-distance` on edge `b0:c0->b1:c0` in SVG only."""
    pat = re.compile(
        r'(data-edge-id="b0:c0-(?:>|&gt;)b1:c0"[^>]*data-distance=")0\.25(")',
        flags=re.DOTALL,
    )
    mutated, count = pat.subn(r"\g<1>0.299999\2", html_text, count=1)
    assert count == 1, "Failed to mutate SVG data-distance"
    return mutated, run_id


def tamper_04_simultaneous_svg_and_json_distance_drift(html_text: str, run_id: str) -> tuple[str, str]:
    """Mutate `distance` identically in BOTH SVG `data-distance` AND embedded JSON vs DuckDB ground truth."""
    mutated, _ = tamper_03_svg_distance_drift(html_text, run_id)

    def mut(payload: dict[str, Any]) -> None:
        payload["edges"]["b0:c0->b1:c0"]["distance"] = 0.299999

    return _replace_provenance_json(mutated, mut), run_id


def tamper_05_secondary_split_to_primary_swap(html_text: str, run_id: str) -> tuple[str, str]:
    """Swap `secondary_split` -> `primary` on `b0:c1->b1:c2` in both SVG and JSON."""
    pat = re.compile(
        r'(data-edge-id="b0:c1-(?:>|&gt;)b1:c2"[^>]*data-edge-role=")secondary_split(")',
        flags=re.DOTALL,
    )
    mutated, count = pat.subn(r"\g<1>primary\2", html_text, count=1)
    assert count == 1, "Failed to swap edge role in SVG"

    def mut(payload: dict[str, Any]) -> None:
        payload["edges"]["b0:c1->b1:c2"]["edge_role"] = "primary"

    return _replace_provenance_json(mutated, mut), run_id


def tamper_06_cross_gap_edge_injection(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject an illegal cross-gap `.evo-edge` spanning across missing batch `b3` (`b2:c0->b4:c0`)."""
    injected_edge = (
        f'    <g class="evo-edge edge-role-primary" tabindex="0" role="button" '
        f'data-edge-id="b2:c0-&gt;b4:c0" data-evolution-run-id="{run_id}" '
        f'data-prev-node-id="b2:c0" data-curr-node-id="b4:c0" data-edge-role="primary" '
        f'data-event-type="continuation" data-distance="0.3" data-distance-over-scale="0.3" '
        f'data-attributed-flow="0.3" data-ambiguous-tie="false" data-high-uncertainty="false">'
        f'<path class="evo-edge-ribbon" d="M 480 120 L 900 120" stroke="#2563eb" /></g>\n'
    )
    marker = '<g class="evo-edges-layer">\n'
    assert marker in html_text
    return html_text.replace(marker, marker + injected_edge, 1), run_id


def tamper_07_external_script_url(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject an external `<script src="https://...">` reference."""
    ext_tag = '  <script src="https://cdn.example.invalid/d3.v7.min.js"></script>\n</head>'
    assert "</head>" in html_text
    return html_text.replace("</head>", ext_tag, 1), run_id


def tamper_08_hidden_or_removed_disclaimer(html_text: str, run_id: str) -> tuple[str, str]:
    """Hide `#ensabc-disclaimer-banner` via `style="display:none"`."""
    target = '<header id="ensabc-disclaimer-banner"'
    assert target in html_text
    return html_text.replace(target, '<header id="ensabc-disclaimer-banner" style="display:none"', 1), run_id


def tamper_09_stripped_margin_or_rule_basis(html_text: str, run_id: str) -> tuple[str, str]:
    """Strip `optimality_margin` and `rule_basis` from an edge in embedded provenance JSON."""

    def mut(payload: dict[str, Any]) -> None:
        edge = payload["edges"]["b0:c1->b1:c2"]
        del edge["optimality_margin"]
        del edge["rule_basis"]

    return _replace_provenance_json(html_text, mut), run_id


def tamper_10_wrong_evolution_run_id(html_text: str, run_id: str) -> tuple[str, str]:
    """Verify against a mismatched `evolution_run_id`."""
    wrong_id = "ffff0000" + run_id[8:]
    return html_text, wrong_id


def tamper_11_raw_angle_bracket_in_provenance_json(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject an unescaped raw '<' / '>' inside <script id='ensabc-evolution-provenance'>."""

    def mut(payload: dict[str, Any]) -> None:
        payload["transitions"][2]["gap_reason"] = "curr_unavailable <raw_tag>"

    return _replace_provenance_json(html_text, mut, escape_html_chars=False), run_id


def tamper_12_extra_csp_directive_img_src(html_text: str, run_id: str) -> tuple[str, str]:
    """Append an extra CSP directive `img-src https:`."""
    old_csp = "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';"
    assert old_csp in html_text
    return (
        html_text.replace(old_csp, old_csp + " img-src https:;", 1),
        run_id,
    )


def tamper_13_css_external_url_in_style(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject a CSS `url(https://...)` rule inside `<style>`."""
    rule = "    .meta-strip { background-image: u/*c*/rl(https://evil.example.invalid/t.png); }\n  </style>"
    assert "</style>" in html_text
    return html_text.replace("</style>", rule, 1), run_id


def tamper_14_extra_svg_island(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject a second `<svg>` island outside `#ensabc-evolution-svg`."""
    extra_svg = '<svg id="extra-svg" xmlns="http://www.w3.org/2000/svg" width="10" height="10"></svg>\n</body>'
    assert "</body>" in html_text
    return html_text.replace("</body>", extra_svg, 1), run_id


def tamper_15_fake_data_decorative_path(html_text: str, run_id: str) -> tuple[str, str]:
    """Inject a rogue `<path data-decorative='true'>` attempting to spoof decorative ownership."""
    fake_dec = '\n  <path data-decorative="true" d="M 10 10 L 95 95" stroke="#111" />\n</svg>'
    assert "</svg>" in html_text
    return html_text.replace("</svg>", fake_dec, 1), run_id


def tamper_16_tampered_provenance_ledger_cell(html_text: str, run_id: str) -> tuple[str, str]:
    """Tamper a visible cell inside `<details id='ensabc-provenance-ledger'>`."""
    old_row = "<tr><th>secondary_rule</th><td><code>nearest_feasible_v1</code></td></tr>"
    new_row = "<tr><th>secondary_rule</th><td><code>tampered_rule_v99</code></td></tr>"
    assert old_row in html_text
    return html_text.replace(old_row, new_row, 1), run_id


def tamper_17_descendant_display_none_in_disclaimer(html_text: str, run_id: str) -> tuple[str, str]:
    """Hide the `<ul>` clause list inside `#ensabc-disclaimer-banner` via descendant inline style."""
    old_ul = "</h1>\n    <ul>"
    new_ul = '</h1>\n    <ul style="display:none">'
    assert old_ul in html_text
    return html_text.replace(old_ul, new_ul, 1), run_id


def tamper_18_attribute_selector_css_hide_disclaimer(html_text: str, run_id: str) -> tuple[str, str]:
    """Hide `#ensabc-disclaimer-banner` via CSS attribute selector `[id=\"ensabc-disclaimer-banner\"]`."""
    rule = '    /* cloak */ [id="ensabc-disclaimer-banner"] { display: none; }\n  </style>'
    assert "</style>" in html_text
    return html_text.replace("</style>", rule, 1), run_id


TAMPER_MUTATIONS: tuple[tuple[str, str, Callable[[str, str], tuple[str, str]]], ...] = (
    ("tamper_01_deleted_svg_node", "pass_3_svg_json_bijection", tamper_01_deleted_svg_node),
    ("tamper_02_injected_orphan_path", "pass_2_svg_xml_zero_orphans", tamper_02_injected_orphan_path),
    ("tamper_03_svg_distance_drift", "pass_3_svg_json_bijection", tamper_03_svg_distance_drift),
    (
        "tamper_04_simultaneous_svg_and_json_distance_drift",
        "pass_4_duckdb_ground_truth",
        tamper_04_simultaneous_svg_and_json_distance_drift,
    ),
    (
        "tamper_05_secondary_split_to_primary_swap",
        "pass_4_duckdb_ground_truth",
        tamper_05_secondary_split_to_primary_swap,
    ),
    ("tamper_06_cross_gap_edge_injection", "pass_2_svg_xml_zero_orphans", tamper_06_cross_gap_edge_injection),
    ("tamper_07_external_script_url", "pass_1_offline_csp", tamper_07_external_script_url),
    (
        "tamper_08_hidden_or_removed_disclaimer",
        "pass_5_disclaimer_and_claims",
        tamper_08_hidden_or_removed_disclaimer,
    ),
    ("tamper_09_stripped_margin_or_rule_basis", "pass_3_svg_json_bijection", tamper_09_stripped_margin_or_rule_basis),
    ("tamper_10_wrong_evolution_run_id", "pass_3_svg_json_bijection", tamper_10_wrong_evolution_run_id),
    (
        "tamper_11_raw_angle_bracket_in_provenance_json",
        "pass_3_svg_json_bijection",
        tamper_11_raw_angle_bracket_in_provenance_json,
    ),
    ("tamper_12_extra_csp_directive_img_src", "pass_1_offline_csp", tamper_12_extra_csp_directive_img_src),
    ("tamper_13_css_external_url_in_style", "pass_1_offline_csp", tamper_13_css_external_url_in_style),
    ("tamper_14_extra_svg_island", "pass_2_svg_xml_zero_orphans", tamper_14_extra_svg_island),
    ("tamper_15_fake_data_decorative_path", "pass_2_svg_xml_zero_orphans", tamper_15_fake_data_decorative_path),
    (
        "tamper_16_tampered_provenance_ledger_cell",
        "pass_3_svg_json_bijection",
        tamper_16_tampered_provenance_ledger_cell,
    ),
    (
        "tamper_17_descendant_display_none_in_disclaimer",
        "pass_5_disclaimer_and_claims",
        tamper_17_descendant_display_none_in_disclaimer,
    ),
    (
        "tamper_18_attribute_selector_css_hide_disclaimer",
        "pass_5_disclaimer_and_claims",
        tamper_18_attribute_selector_css_hide_disclaimer,
    ),
)


def run_self_check(*, out_report: Path | None = None) -> dict[str, Any]:
    """Execute Pass 6: baseline positive verification + all 10 tamper cases + incomplete provenance rejection."""
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="ensabc-m3-viz-selfcheck-") as tmp_str:
        tmp = Path(tmp_str)
        db_path = tmp / "showcase.duckdb"
        seed_showcase_duckdb(db_path)

        baseline_html = tmp / "baseline.html"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_render = render_cli_main(
                ["--db", str(db_path), "--evolution-run-id", SHOWCASE_RUN_ID, "--output", str(baseline_html)]
            )
        if rc_render != 0 or not baseline_html.is_file():
            raise RuntimeError(f"Self-check baseline render failed with exit code {rc_render}")

        baseline_audit = verify_html_traceability(baseline_html, db_path, SHOWCASE_RUN_ID)
        baseline_ok = baseline_audit["status"] == "pass"

        baseline_text = baseline_html.read_text(encoding="utf-8")
        tamper_results: list[dict[str, Any]] = []

        for case_name, expected_pass_key, mutator in TAMPER_MUTATIONS:
            mut_text, target_run_id = mutator(baseline_text, SHOWCASE_RUN_ID)
            tamper_path = tmp / f"{case_name}.html"
            tamper_path.write_text(mut_text, encoding="utf-8")
            rc = main(
                [
                    "--html",
                    str(tamper_path),
                    "--db",
                    str(db_path),
                    "--evolution-run-id",
                    target_run_id,
                ]
            )
            audit = verify_html_traceability(tamper_path, db_path, target_run_id)
            expected_pass_failed = audit["passes"][expected_pass_key]["status"] == "fail"
            rejected = rc == 1 and audit["status"] == "fail" and expected_pass_failed
            tamper_results.append(
                {
                    "case": case_name,
                    "exit_code": rc,
                    "audit_status": audit["status"],
                    "expected_catching_pass": expected_pass_key,
                    "expected_pass_caught": expected_pass_failed,
                    "rejected_as_expected": rejected,
                    "sample_finding": audit["findings"][0] if audit["findings"] else "",
                }
            )

        # Also test removed disclaimer banner explicitly as sub-check of #8
        removed_banner_html = re.sub(
            r'<header id="ensabc-disclaimer-banner".*?</header>',
            "",
            baseline_text,
            count=1,
            flags=re.DOTALL,
        )
        rem_path = tmp / "tamper_08b_removed_disclaimer.html"
        rem_path.write_text(removed_banner_html, encoding="utf-8")
        rc_rem = main(
            ["--html", str(rem_path), "--db", str(db_path), "--evolution-run-id", SHOWCASE_RUN_ID]
        )
        removed_disclaimer_rejected = rc_rem == 1

        # Incomplete provenance render rejection test (absent run_id & NULL required column)
        missing_out = tmp / "should_not_exist.html"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_missing_run = render_cli_main(
                ["--db", str(db_path), "--evolution-run-id", "nonexistent_run_id", "--output", str(missing_out)]
            )
        zero_bytes_on_missing = (not missing_out.exists()) or missing_out.stat().st_size == 0

        corrupt_db = tmp / "corrupt_null.duckdb"
        proc_corrupt = run_isolated_duckdb(
            """
            CREATE TABLE evolution_runs AS SELECT 'bad_run' AS evolution_run_id, 'rk' AS tracking_run_key,
              NULL AS evolution_binding, [1.0]::DOUBLE[] AS weights, 4 AS planned_batch_count,
              NULL AS chain_summary, 'tok' AS non_physical_disclaimer, 'sha' AS content_sha256;
            CREATE TABLE replay_batches AS SELECT 'rk' AS run_key, 0 AS ordinal,
              TIMESTAMP '2020-01-01 00:00:00' AS init_time, 24 AS lead_hours, 'ok' AS status,
              NULL::VARCHAR AS status_reason, 'i' AS input_sha256, 'o' AS om_sha256,
              1.0::DOUBLE AS dispersion, 'c' AS cluster_result_sha256;
            CREATE TABLE evolution_nodes AS SELECT 'bad_run' AS evolution_run_id, 0 AS ordinal, 0 AS cluster_id,
              0 AS canonical_rank, 'rk0000000000:b00:k00' AS branch_id, [1.0]::DOUBLE[] AS mean_features,
              10 AS member_count, 1.0::DOUBLE AS member_fraction, 'first_batch' AS assignment_reason,
              0 AS in_degree_primary, 0 AS in_degree_secondary, 0 AS out_degree_primary, 0 AS out_degree_secondary,
              'first_batch' AS incoming_role, 'final_batch' AS outgoing_role, 0.0::DOUBLE AS primary_delta_fraction,
              false AS has_primary_delta_fraction, 0.0::DOUBLE AS unattributed_inflow_fraction,
              0.0::DOUBLE AS unattributed_outflow_fraction, false AS high_uncertainty;
            CREATE TABLE evolution_transitions AS SELECT 'bad_run' AS evolution_run_id, 0 AS prev_ordinal,
              1 AS curr_ordinal, 'ok' AS transition_status, false AS chain_broken, NULL::VARCHAR AS gap_reason,
              1.0::DOUBLE AS pair_scale, 1.0::DOUBLE AS threshold, 0 AS primary_edge_count,
              0 AS secondary_split_edge_count, 0 AS secondary_merge_edge_count,
              0.0::DOUBLE AS transition_tv_redistribution, 0.5::DOUBLE AS min_threshold_margin,
              false AS ambiguous_tie, false AS high_uncertainty, {'v': 1} AS evolve_result WHERE false;
            CREATE TABLE evolution_edges AS SELECT 'bad_run' AS evolution_run_id, 0 AS prev_ordinal,
              0 AS prev_cluster_id, 1 AS curr_ordinal, 0 AS curr_cluster_id, 'b' AS prev_branch_id,
              'b' AS curr_branch_id, 'primary' AS edge_role, 'continuation' AS event_type,
              NULL::VARCHAR AS rule_basis, 0.1::DOUBLE AS distance, 0.1::DOUBLE AS distance_over_scale,
              1.0::DOUBLE AS prev_member_fraction, 1.0::DOUBLE AS curr_member_fraction,
              0.0::DOUBLE AS edge_delta_fraction, 1.0::DOUBLE AS attributed_flow, 0.9::DOUBLE AS edge_strength,
              0.9::DOUBLE AS threshold_margin, 0.9::DOUBLE AS threshold_margin_ratio, 'tied' AS primary_alternative_status,
              false AS has_optimality_margin, 0.0::DOUBLE AS optimality_margin, 0.0::DOUBLE AS optimality_margin_ratio,
              false AS has_secondary_margin, 0.0::DOUBLE AS secondary_margin, 0.0::DOUBLE AS secondary_margin_ratio,
              false AS ambiguous_tie, false AS high_uncertainty, []::VARCHAR[] AS uncertainty_reasons,
              'tok' AS non_physical_disclaimer WHERE false;
            CREATE TABLE evolution_events AS SELECT 'bad_run' AS evolution_run_id, 0 AS prev_ordinal,
              0 AS event_index, 'continuation' AS event_type, []::INTEGER[] AS prev_cluster_ids,
              []::INTEGER[] AS curr_cluster_ids, []::VARCHAR[] AS branch_ids, NULL::VARCHAR AS primary_branch_id,
              'r' AS rule_basis, 0.0::DOUBLE AS max_morphology_distance, 0.0::DOUBLE AS max_morphology_over_scale,
              0.0::DOUBLE AS prev_fraction_sum, 0.0::DOUBLE AS curr_fraction_sum, 0.0::DOUBLE AS net_delta_fraction,
              0.0::DOUBLE AS attributed_flow_sum, false AS has_min_margin_ratio, 0.0::DOUBLE AS min_margin_ratio,
              false AS ambiguous_tie, false AS high_uncertainty, 'tok' AS non_physical_disclaimer WHERE false;
            CREATE TABLE evolution_branch_summaries AS SELECT 'bad_run' AS evolution_run_id, 'b' AS branch_id,
              0 AS birth_ordinal, 0 AS end_ordinal, 1 AS node_count, 0 AS step_count, 'first_batch' AS birth_reason,
              'final_batch' AS termination_reason, 0.0::DOUBLE AS cumulative_morphology_distance,
              0.0::DOUBLE AS cumulative_morphology_over_scale, 0.0::DOUBLE AS max_step_morphology_distance,
              0.0::DOUBLE AS max_step_morphology_over_scale, 0.0::DOUBLE AS end_to_end_morphology_distance,
              1.0::DOUBLE AS initial_member_fraction, 1.0::DOUBLE AS final_member_fraction,
              1.0::DOUBLE AS min_member_fraction, 1.0::DOUBLE AS max_member_fraction,
              0.0::DOUBLE AS net_delta_fraction, 0.0::DOUBLE AS cumulative_abs_delta_fraction,
              0 AS split_out_event_count, []::VARCHAR[] AS spawned_secondary_branch_ids,
              NULL::VARCHAR AS split_from_parent_branch_id, 0 AS merge_in_event_count,
              []::VARCHAR[] AS absorbed_secondary_branch_ids, NULL::VARCHAR AS merged_into_branch_id,
              0 AS ambiguous_step_count, 0 AS high_uncertainty_step_count, 0.0::DOUBLE AS high_uncertainty_step_ratio
              WHERE false;
            """,
            database=corrupt_db,
            readonly=False,
        )
        if proc_corrupt.returncode != 0:
            raise RuntimeError(f"Failed creating corrupt provenance test DB: {proc_corrupt.stderr}")

        corrupt_out = tmp / "corrupt_should_not_exist.html"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_corrupt = render_cli_main(
                ["--db", str(corrupt_db), "--evolution-run-id", "bad_run", "--output", str(corrupt_out)]
            )
        zero_bytes_on_corrupt = (not corrupt_out.exists()) or corrupt_out.stat().st_size == 0

        incomplete_provenance_rejected = (
            rc_missing_run == 1
            and zero_bytes_on_missing
            and rc_corrupt == 1
            and zero_bytes_on_corrupt
        )

        # [H5] Safe rendering of '</script><script>alert(1)</script>' in gap_reason & status_reason
        xss_db = tmp / "xss_gap.duckdb"
        proc_xss = run_isolated_duckdb(
            f"""
            ATTACH '{db_path}' AS src (READ_ONLY);
            CREATE TABLE evolution_runs AS SELECT * FROM src.evolution_runs;
            CREATE TABLE replay_batches AS
              SELECT * REPLACE (
                CASE WHEN ordinal = 3 THEN 'MISSING_</script><script>alert(1)</script>' ELSE status_reason END AS status_reason
              ) FROM src.replay_batches;
            CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes;
            CREATE TABLE evolution_transitions AS
              SELECT * REPLACE (
                CASE WHEN prev_ordinal = 2 THEN 'curr_unavailable_</script><script>alert(1)</script>' ELSE gap_reason END AS gap_reason
              ) FROM src.evolution_transitions;
            CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges;
            CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events;
            CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries;
            DETACH src;
            """,
            database=xss_db,
            readonly=False,
        )
        if proc_xss.returncode != 0:
            raise RuntimeError(f"Failed creating XSS gap test DB: {proc_xss.stderr}")
        xss_html = tmp / "xss_safe.html"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_xss = render_cli_main(
                ["--db", str(xss_db), "--evolution-run-id", SHOWCASE_RUN_ID, "--output", str(xss_html)]
            )
        xss_audit = verify_html_traceability(xss_html, xss_db, SHOWCASE_RUN_ID) if xss_html.is_file() else {"status": "fail"}
        script_escape_verified = rc_xss == 0 and xss_audit["status"] == "pass"

        # [M8] evolve_result mismatch in DuckDB caught by Pass 4
        m8_db = tmp / "m8_evolve_mismatch.duckdb"
        proc_m8 = run_isolated_duckdb(
            f"""
            ATTACH '{db_path}' AS src (READ_ONLY);
            CREATE TABLE evolution_runs AS SELECT * FROM src.evolution_runs;
            CREATE TABLE replay_batches AS SELECT * FROM src.replay_batches;
            CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes;
            CREATE TABLE evolution_transitions AS
              SELECT * REPLACE (
                CASE WHEN prev_ordinal = 0 THEN {{
                  'algorithm_version': 'm3.evolve.v1',
                  'summary': evolve_result.summary,
                  'edges': [],
                  'events': evolve_result.events
                }} ELSE evolve_result END AS evolve_result
              ) FROM src.evolution_transitions;
            CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges;
            CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events;
            CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries;
            DETACH src;
            """,
            database=m8_db,
            readonly=False,
        )
        if proc_m8.returncode != 0:
            raise RuntimeError(f"Failed creating M8 mismatch DB: {proc_m8.stderr}")
        m8_audit = verify_html_traceability(baseline_html, m8_db, SHOWCASE_RUN_ID)
        evolve_result_mismatch_caught = (
            m8_audit["status"] == "fail"
            and m8_audit["passes"]["pass_4_duckdb_ground_truth"]["status"] == "fail"
        )

        # [M9] 0-ok-batch all-gap run renders cleanly and passes all 5 verification passes
        all_gap_db = tmp / "all_gap_zero_ok.duckdb"
        all_gap_erid = "0000111122223333444455556666777788889999aaaabbbbccccddddeeeeffff"
        all_gap_trk = "allgaprk1234567890"
        proc_gap = run_isolated_duckdb(
            f"""
            ATTACH '{db_path}' AS src (READ_ONLY);
            CREATE TABLE evolution_runs AS
              SELECT
                '{all_gap_erid}' AS evolution_run_id,
                '{all_gap_trk}' AS tracking_run_key,
                {{
                  'tracking_run_key': '{all_gap_trk}',
                  'input_manifest_sha256': '1111111111111111111111111111111111111111111111111111111111111111',
                  'scenario_config_sha256': '2222222222222222222222222222222222222222222222222222222222222222',
                  'match_config_sha256': '3333333333333333333333333333333333333333333333333333333333333333',
                  'evolve_config_sha256': '7777777777777777777777777777777777777777777777777777777777777777',
                  'weights_version': 'cos_lat_center_v1',
                  'secondary_rule': 'nearest_feasible_v1',
                  'flow_rule': 'star_min_mass_proportional_v1',
                  'uncertainty_rule': 'm3.uncertainty.v1',
                  'fraction_sum_tolerance': 1e-9::DOUBLE,
                  'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                  'high_uncertainty_optimality_ratio': 0.10::DOUBLE,
                  'high_uncertainty_secondary_ratio': 0.10::DOUBLE,
                  'software': src.evolution_runs.evolution_binding.software
                }} AS evolution_binding,
                weights,
                8 AS planned_batch_count,
                {{
                  'ok_batch_count': 0,
                  'missing_batch_count': 0,
                  'failed_batch_count': 8,
                  'ok_transition_count': 0,
                  'gap_transition_count': 7,
                  'branch_count': 0,
                  'primary_edge_count': 0,
                  'secondary_split_edge_count': 0,
                  'secondary_merge_edge_count': 0,
                  'continuation_count': 0,
                  'split_count': 0,
                  'merge_count': 0,
                  'emergence_count': 0,
                  'disappearance_count': 0,
                  'gap_interrupted_count': 0,
                  'gap_restarted_count': 0,
                  'cumulative_primary_distance': 0.0::DOUBLE,
                  'max_edge_distance': 0.0::DOUBLE,
                  'max_edge_distance_over_scale': 0.0::DOUBLE,
                  'mean_transition_tv_redistribution': 0.0::DOUBLE,
                  'max_transition_tv_redistribution': 0.0::DOUBLE,
                  'ambiguous_transition_count': 0,
                  'high_uncertainty_transition_count': 0,
                  'high_uncertainty_transition_ratio': 0.0::DOUBLE,
                  'high_uncertainty_edge_count': 0,
                  'high_uncertainty_edge_ratio': 0.0::DOUBLE
                }} AS chain_summary,
                non_physical_disclaimer,
                'abababababababababababababababababababababababababababababababab' AS content_sha256
              FROM src.evolution_runs;
            CREATE TABLE replay_batches AS
              SELECT
                '{all_gap_trk}' AS run_key,
                ordinal,
                init_time,
                lead_hours,
                'failed' AS status,
                'INSUFFICIENT_RANK' AS status_reason,
                'in' || ordinal::VARCHAR AS input_sha256,
                'om' || ordinal::VARCHAR AS om_sha256,
                NULL::DOUBLE AS dispersion,
                NULL::VARCHAR AS cluster_result_sha256
              FROM src.replay_batches;
            CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes WHERE false;
            CREATE TABLE evolution_transitions AS
              SELECT
                '{all_gap_erid}' AS evolution_run_id,
                prev_ordinal,
                curr_ordinal,
                'gap' AS transition_status,
                true AS chain_broken,
                'both_unavailable' AS gap_reason,
                NULL::DOUBLE AS pair_scale,
                NULL::DOUBLE AS threshold,
                0 AS primary_edge_count,
                0 AS secondary_split_edge_count,
                0 AS secondary_merge_edge_count,
                NULL::DOUBLE AS transition_tv_redistribution,
                NULL::DOUBLE AS min_threshold_margin,
                false AS ambiguous_tie,
                false AS high_uncertainty,
                NULL AS evolve_result
              FROM src.evolution_transitions;
            CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges WHERE false;
            CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events WHERE false;
            CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries WHERE false;
            DETACH src;
            """,
            database=all_gap_db,
            readonly=False,
        )
        if proc_gap.returncode != 0:
            raise RuntimeError(f"Failed creating 0-ok-batch DB: {proc_gap.stderr}")
        all_gap_html = tmp / "all_gap.html"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_gap_render = render_cli_main(
                ["--db", str(all_gap_db), "--evolution-run-id", all_gap_erid, "--output", str(all_gap_html)]
            )
        all_gap_audit = (
            verify_html_traceability(all_gap_html, all_gap_db, all_gap_erid)
            if all_gap_html.is_file()
            else {"status": "fail"}
        )
        zero_ok_batch_all_gap_verified = rc_gap_render == 0 and all_gap_audit["status"] == "pass"

    all_tampers_rejected = all(item["rejected_as_expected"] for item in tamper_results)
    overall_ok = (
        baseline_ok
        and len(tamper_results) == len(TAMPER_MUTATIONS)
        and all_tampers_rejected
        and removed_disclaimer_rejected
        and incomplete_provenance_rejected
        and script_escape_verified
        and evolve_result_mismatch_caught
        and zero_ok_batch_all_gap_verified
    )
    summary = {
        "schema_version": SELF_CHECK_SCHEMA_VERSION,
        "status": "pass" if overall_ok else "fail",
        "checked_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "baseline_showcase_passed": baseline_ok,
        "tamper_cases_tested": len(tamper_results),
        "tamper_cases_rejected": sum(1 for item in tamper_results if item["rejected_as_expected"]),
        "removed_disclaimer_rejected": removed_disclaimer_rejected,
        "incomplete_provenance_render_rejected": incomplete_provenance_rejected,
        "script_escape_verified": script_escape_verified,
        "evolve_result_mismatch_caught": evolve_result_mismatch_caught,
        "zero_ok_batch_all_gap_verified": zero_ok_batch_all_gap_verified,
        "cases": tamper_results,
    }
    if out_report is not None:
        atomic_write_json(out_report, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="M3 6-pass visualization traceability verifier and 10-case tamper self-check."
    )
    parser.add_argument("--html", type=Path, default=None, help="Rendered .html file to verify")
    parser.add_argument("--db", type=Path, default=None, help="Source DuckDB database path")
    parser.add_argument("--evolution-run-id", type=str, default=None, help="Expected evolution_run_id")
    parser.add_argument("--out-report", type=Path, default=None, help="Optional JSON audit report path")
    parser.add_argument("--self-check", action="store_true", help="Run 10-case negative tamper self-check")
    args = parser.parse_args(argv)

    if args.self_check:
        summary = run_self_check(out_report=args.out_report)
        print(
            f"Visualization self-check: {summary['status'].upper()} "
            f"({summary['tamper_cases_rejected']}/{summary['tamper_cases_tested']} tamper mutations rejected, "
            f"incomplete_provenance_rejected={summary['incomplete_provenance_render_rejected']})"
        )
        return 0 if summary["status"] == "pass" else 1

    if args.html is None or args.db is None or not args.evolution_run_id:
        print(
            "Error: --html, --db, and --evolution-run-id are required unless --self-check is passed",
            file=sys.stderr,
        )
        return 2
    if not args.html.is_file():
        print(f"Error: HTML file not found: {args.html}", file=sys.stderr)
        return 2
    if not args.db.is_file():
        print(f"Error: DuckDB file not found: {args.db}", file=sys.stderr)
        return 2

    report = verify_html_traceability(
        args.html,
        args.db,
        args.evolution_run_id,
        out_report=args.out_report,
    )
    if argv is None:
        print(
            f"Traceability audit: {report['status'].upper()} "
            f"({len(report['findings'])} findings)"
        )
        for finding in report["findings"][:10]:
            print(f"  - {finding}", file=sys.stderr)
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
