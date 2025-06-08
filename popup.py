from influxdb_client import InfluxDBClient
import os
import time
from dotenv import load_dotenv
import pygame
from pygame.locals import QUIT

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
org = "iit_bh"
token = "GEfq64vm4n-7tQ7F8AoGf5OfKlJ41hEF84NdUZ-daQJ_lIBFCzqrFMnIMCL57Dm4GGTCFyUJQsxkTeypWfq0EQ=="
url = "http://127.0.0.1:8086"
bucket = "iba_data_10mm"
key = "CVAH_L1"

# client = InfluxDBClient(url=url, token=token, timeout=25000)
client = InfluxDBClient(url=url, token=token)
query_api = client.query_api()

bucket = "iba_data_10mm"
measurement = key+"_anomaly_score_cobble"
start_time = "2024-02-15T02:20:00.000Z"
end_time = "2024-02-15T02:30:00.000Z"
field = "predicted_cobble"

    

while True:
    # Query to check for anomalies
    query_for_anomaly_time = (
        'from(bucket: "' + bucket + '")'
        '|> range(start: ' + start_time + ', stop: ' + end_time + ' )'
        '|> filter(fn: (r) => r["_measurement"] == "' + measurement + '")'
        '|> filter(fn: (r) => r["_field"] == "predicted_cobble")'
        '|> filter(fn: (r) => r["_value"] == 1'
    )
    query_for_anomaly_time += ')'
    
    # Execute the query
    result = query_api.query(org=org, query=query_for_anomaly_time)
    print(result)
    # Check if result is not empty
    if result:
        print("Anomaly detected!")
        
        break  # Break out of the loop if you only want to find one anomaly
    time.sleep(10)  # Check every 10 seconds

# for table in result:
#             for record in table.records:
#               row = ''.join((str(record.get_time()),", ", record.get_field(),", ",str(record.get_value()),"\n"))







# Initialize pygame and mixer for sound
pygame.init()
pygame.mixer.init()

# Colors
BLACK = (0, 0, 0)
RED = (255, 0, 0)
WHITE = (255, 255, 255)

# Set up display
screen = pygame.display.set_mode((640, 540))

# Font setup
font = pygame.font.SysFont(None, 38)
font_large = pygame.font.SysFont(None, 72)  # Larger font for "ALERT"

# Load and play sound (update the path to your sound file)
try:
    alert_sound = pygame.mixer.Sound('stand_1-6 and stand 13_18 .mp3')  # Use the file path of your sound file
    alert_sound.play(loops=-1)  # Play the sound in a loop (continuous)
except pygame.error as e:
    print(f"Error loading sound file: {e}")

# Setup text rendering
text_color = WHITE
background = RED
running = True

while running:
    for event in pygame.event.get():
        if event.type == QUIT:
            running = False

    # Fill the screen with background color
    screen.fill(background)
    
#  Create text surfaces
    alert_text = font_large.render("ALERT !!!", True, text_color)
    profile_text = font.render(f"Profile: {bucket}", True, text_color)
    time_text = font.render(f"Time: {start_time}", True, text_color)  # Fixed variable name
    region_text = font.render(f"Region: {key}", True, text_color)
    sensor_text = font.render(f"Sensor: ", True, text_color)
    
    # Position texts on the screen
    screen.blit(alert_text, (20, 20))  # "ALERT !!!" at top
    screen.blit(time_text, (20, 150))    # Time text below Profile text
    screen.blit(profile_text, (20, 100))  # Profile text below "ALERT !!!"
    screen.blit(region_text, (20, 200))   # Field Value text below Time text
    screen.blit(sensor_text, (20, 250))   # Sensor Value text below Time text

    # Update display
    pygame.display.update()

# Stop the sound when the program quits
alert_sound.stop()

pygame.quit()


