from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import numpy as np
import fastf1

import matplotlib.pyplot as plt

session = fastf1.get_session(2024, 'Monaco', 'Q')
session.load()

lap = session.laps.pick_fastest()
pos = lap.get_pos_data()

circuit_info = session.get_circuit_info()

def rotate(xy, *, angle):
    rot_mat = np.array([[np.cos(angle), np.sin(angle)],
                        [-np.sin(angle), np.cos(angle)]])
    return np.matmul(xy, rot_mat)


track = pos.loc[:, ('X', 'Y')].to_numpy()

track_angle = circuit_info.rotation / 180 * np.pi


rotated_track = rotate(track, angle=track_angle)
plt.plot(rotated_track[:, 0], rotated_track[:, 1])

offset_vector = [500, 0]  


for _, corner in circuit_info.corners.iterrows():

    txt = f"{corner['Number']}{corner['Letter']}"


    offset_angle = corner['Angle'] / 180 * np.pi

    
    offset_x, offset_y = rotate(offset_vector, angle=offset_angle)


    text_x = corner['X'] + offset_x
    text_y = corner['Y'] + offset_y


    text_x, text_y = rotate([text_x, text_y], angle=track_angle)


    track_x, track_y = rotate([corner['X'], corner['Y']], angle=track_angle)


    plt.scatter(text_x, text_y, color='grey', s=140)


    plt.plot([track_x, text_x], [track_y, text_y], color='grey')


    plt.text(text_x, text_y, txt,
             va='center_baseline', ha='center', size='small', color='white')

plt.xticks([])
plt.yticks([])
plt.axis('equal')


plt.box(False)
plt.title('')
plt.savefig('monaco.svg')

plt.show()


#####
import json

lap = session.laps.pick_fastest()
pos = lap.get_pos_data()
circuit_info = session.get_circuit_info()


track_data = pos.loc[:, ('X', 'Y')].to_numpy()
track_list = [{'x': float(x), 'y': float(y)} for x, y in track_data]


corners_list = []
for _, corner in circuit_info.corners.iterrows():
    corners_list.append({
        'number': corner['Number'],
        'letter': corner['Letter'],
        'x': float(corner['X']),
        'y': float(corner['Y']),
        'angle': float(corner['Angle'])
    })


circuit_info_dict = {
    'rotation': float(circuit_info.rotation)
}


data = {
    'trackData': track_list,
    'cornersData': corners_list,
    'circuitInfo': circuit_info_dict
}


with open('circuit_data.json', 'w') as json_file:
    json.dump(data, json_file, indent=4)


