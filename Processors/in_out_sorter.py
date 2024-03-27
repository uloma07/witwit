import os
import pandas as pd
import requests
import torch
import timm
import torchvision.transforms as transforms
from PIL import Image

# Check if CUDA is available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Initialize the pre-trained model
model = timm.create_model('resnext101_32x8d_wsl', pretrained=True)
model.to(device)  # Move the model to the appropriate device
model.eval()  # Set the model to evaluation mode

# Define the transformations for image preprocessing
transform = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
])

# Function to predict the class of an image
def predict_image(image_path):
    image = Image.open(image_path)
    image_tensor = transform(image).unsqueeze(0).to(device)  # Move the input tensor to the appropriate device
    with torch.no_grad():
        output = model(image_tensor)
    _, predicted = torch.max(output.data, 1)
    class_idx = predicted.item()
    if class_idx == 365:  # Outdoor
        return 'outdoor'
    else:  # Indoor
        return 'indoor'

# Read the input CSV file
input_df = pd.read_csv('Data/raw/image_data.csv')

# Create empty lists to store indoor and outdoor data
indoor_data = []
outdoor_data = []

# Loop through the input data and classify each image
for _, row in input_df.iterrows():
    image_url = row['url']
    image_path = os.path.join('images', os.path.basename(image_url))

    # Download the image if not already present
    if not os.path.exists(image_path):
        try:
            response = requests.get(image_url)
            with open(image_path, 'wb') as f:
                f.write(response.content)
        except:
            print(f"Failed to download {image_url}")
            continue

    # Predict the class of the image
    prediction = predict_image(image_path)

    # Append the data to the appropriate list based on the predicted class
    if prediction.startswith('indoor'):
        indoor_data.append(row)
    elif prediction.startswith('outdoor'):
        outdoor_data.append(row)

# Create the indoor and outdoor CSV files
indoor_df = pd.DataFrame(indoor_data)
indoor_df.to_csv('Data/processed/indoor.csv', index=False)

outdoor_df = pd.DataFrame(outdoor_data)
outdoor_df.to_csv('Data/processed/outdoor.csv', index=False)