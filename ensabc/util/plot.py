import math
from typing import Iterator,Tuple
import numpy as np

from matplotlib import pyplot as plt

def iterate_stamp_plot(
        enumerate_iter:Iterator[Tuple[int, np.array]],
        x:list, 
        y:list, 
        row:int=6, 
        total:int=None, 
        subtitle_pattern='idx: {idx}',
        suptitle='',
        figsize=(18,18), 
        **kwargs
) -> plt.figure:
    """generate stamp plot from enumerate_iter object

    Args:
        enumerate_iter (Iterator[Tuple[int, np.array]]): _description_
        x (list): _description_
        y (list): _description_
        row (int, optional): _description_. Defaults to 6.
        total (int, optional): _description_. Defaults to None.
        subtitle_pattern (str, optional): _description_. Defaults to 'idx: {idx}'.
        suptitle (str, optional): _description_. Defaults to ''.
        figsize (tuple, optional): _description_. Defaults to (18,18).

    Returns:
        plt.figure: _description_
    """
    if total is None:
        enumerate_iter = list(enumerate_iter)
        total = len(enumerate_iter)
    col=math.ceil((total)/row)
    n=0
    fig, axes = plt.subplots(row,col,figsize=figsize)
    for i, data in enumerate_iter:
        if row!=1 and col!=1:
            index = int(n/col), n%col
        else:
            index = n
        im = axes[index].contourf(
            x,
            y,
            data,
            **kwargs
        )
        if int(n/col) != row-1:
            axes[index].set_xticks([])
        if int(n%col) != 0:
            axes[index].set_yticks([])
        
        if isinstance(subtitle_pattern, str):
            axes[index].set_title(subtitle_pattern.format(idx=i))
        elif isinstance(subtitle_pattern, list):
            axes[index].set_title(subtitle_pattern[n].format(idx=i))
        n += 1
    
    for i in range(n, row*col):
        if row!=1 and col!=1:
            index = int(i/col),i%col
        else:
            index=i
        axes[index].set_axis_off()
    fig.colorbar(im, ax=axes.ravel().tolist())
    plt.suptitle(suptitle,x=0.4,y=.09, size=15)
    return fig