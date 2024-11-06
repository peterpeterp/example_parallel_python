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

# create empty file to be filled
out_csv_file_name = f"output/output.csv"    
with open(out_csv_file_name, 'w') as fl:
    fl.write('index,lat,lon,result1,result2\n')

# define what has to be done with each grid-cell
def do_grid_cell(coords: tuple):
    index,lat,lon = coords
    data_of_grid_cell = gridded_data.loc[:,lat,lon]

    # do your calculation
    result1 = float(data_of_grid_cell[0])
    result2 = float(data_of_grid_cell.sum('time')) * 3
    result2 += result1

    # lock the csv file
    lock.acquire()
    # add a line
    with open(out_csv_file_name, 'a') as fl:
        fl.write(f"{int(index)},{lat},{lon},{result1},{result2}\n")
    # unlock the csv file
    lock.release()


# this is needed to "lock" the output.csv files
# without this, processes will crash if multiple processes try to write to this file at the same time
def init(l):
    global lock
    lock = l

# you can use less processes and test on a different server
processes = 64
p = multiprocessing.Pool(processes, initializer=init, initargs=(multiprocessing.Lock(),))
p.map(do_grid_cell, grid_cells)
