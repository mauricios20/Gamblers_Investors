# Importing library
import qrcode
import os

# Define working directory
path = "/Users/mau/Library/CloudStorage/Dropbox/Mac/Downloads"
os.chdir(path)
 
# Data to be encoded
data = 'www.splashcarwashdetail.com'
 
# Encoding data using make() function
img = qrcode.make(data)

# Resize the image
img = img.resize((400, 400))

# Displaying the image
img.show()

# Saving as an image file
img.save('splashqrcode2.png')