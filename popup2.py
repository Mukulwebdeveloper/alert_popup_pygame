from influxdb_client import InfluxDBClient
import os
import time
from dotenv import load_dotenv
import pygame
from pygame.locals import QUIT

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
org = os.getenv("ORG")
token = os.getenv("TOKEN")
url = os.getenv("URL")
bucket = os.getenv("BUCKET")
key = os.getenv("KEY")

# Initialize InfluxDB client
client = InfluxDBClient(url=url, token=token)
query_api = client.query_api()

# Define other variables
measurement = f"{key}_anomaly_score_cobble"
start_time = "2024-02-15T02:20:00.000Z"
end_time = "2024-02-15T02:30:00.000Z"
field = "predicted_cobble"

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
font = pygame.font.SysFont(None, 48)
font_large = pygame.font.SysFont(None, 72)  # Larger font for "ALERT"

# Load and play sound (update the path to your sound file)
try:
    alert_sound = pygame.mixer.Sound('stand_1-6 and stand 13_18 .mp3')  # Use the file path of your sound file
except pygame.error as e:
    print(f"Error loading sound file: {e}")
    alert_sound = None  # Prevent further errors if sound loading fails

# Start the loop to check for anomalies and display alerts
running = True
anomaly_detected = False  # Flag to manage sound and display

while running:
    # Query to check for anomalies
    query_for_anomaly_time = f'''
        from(bucket: "{bucket}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["_field"] == "{field}")
        |> filter(fn: (r) => r["_value"] == 1)
    '''

    # Execute the query
    result = query_api.query(org=org, query=query_for_anomaly_time)
    
    if result:  # Check if the result is not empty
        anomaly_detected = True
        print("Anomaly detected!")
        if alert_sound:
            alert_sound.play(loops=-1)  # Start playing the sound
    else:
        anomaly_detected = False
        if alert_sound:
            alert_sound.stop()  # Stop the sound if no anomaly is detected
    
    # Handle Pygame events
    for event in pygame.event.get():
        if event.type == QUIT:
            running = False

    if anomaly_detected:
        # Fill the screen with background color
        screen.fill(RED)
        
        # Create text surfaces
        alert_text = font_large.render("ALERT !!!", True, WHITE)
        profile_text = font.render(f"Profile: {bucket}", True, WHITE)
        time_text = font.render(f"Time: {start_time}", True, WHITE)
        region_text = font.render(f"Region: {key}", True, WHITE)
        sensor_text = font.render(f"Sensor: {field}", True, WHITE)
        
        # Position texts on the screen
        screen.blit(alert_text, (20, 20))  # "ALERT !!!" at top
        screen.blit(profile_text, (20, 100))  # Profile text below "ALERT !!!"
        screen.blit(time_text, (20, 150))  # Time text below Profile text
        screen.blit(region_text, (20, 200))  # Region text below Time text
        screen.blit(sensor_text, (20, 250))  # Sensor text below Region text
        
        # Update display
        pygame.display.update()

    # Pause before the next query
    time.sleep(10)  # Check every 10 seconds

# Stop the sound and quit pygame when the program exits
if alert_sound:
    alert_sound.stop()
pygame.quit()
