import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import os

# manually copy the path here but make sure to keep the r in front so it processes the path correctly
img_path = r"../data/canvas-data/Global Health +AI course fall 2025\images_cropped_padded\UNY956_20240703075947.jpg"

# read and show
img = mpimg.imread(img_path)
plt.imshow(img)
plt.axis("off") 
plt.title("")                   # optionally you can fill this out if you want and put the species name down
plt.show()


