import xarray as xr
import numpy as np
import itertools
import multiprocessing

# read the input data once
gridded_data = xr.open_dataset('/climca/data/CESM2_LE/TREFHTMN/mon_raw/b.e21.BHISTcmip6.f09_g17.LE2-1001.001.cam.h0.TREFHTMN.201001-201412.nc')['TREFHTMN']
# for the example we only use a part of the data
gridded_data = gridded_data.loc[:,10:20,10:20].load()

# create list containing index,lat,lon
grid_cells = np.array([(int(i),c[0],c[1]) for i,c in enumerate([(y,x) 
                        for y,x in itertools.product(gridded_data.lat, gridded_data.lon)])])
print(grid_cells[:4])
'''
[[ 0.         10.83769634 10.        ]
 [ 1.         10.83769634 11.25      ]
 [ 2.         10.83769634 12.5       ]
 [ 3.         10.83769634 13.75      ]]
'''

# define what has to be done with each grid-cell
def do_grid_cell(coords: tuple):
    index,lat,lon = coords
    data_of_grid_cell = gridded_data.loc[:,lat,lon]

    result1 = data_of_grid_cell.cumsum()

    xr.Dataset({'result1':result1}).to_netcdf(f"output/nc/result1_{int(index)}.nc")


# you can use less processes and test on a different server
processes = 64
p = multiprocessing.Pool(processes, initializer=init, initargs=(multiprocessing.Lock(),))
p.map(do_grid_cell, grid_cells)
