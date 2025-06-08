import pygame
from pygame.locals import *
import csv

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
    
    # Create text surfaces
    alert_text = font_large.render("ALERT !!!", True, text_color)
    profile_text = font.render(f"Profile: ", True, text_color)
    time_text = font.render(f"Time: ", True, text_color)  # Fixed variable name
    field_text = font.render(f"Region: ", True, text_color)
    
    # Position texts on the screen
    screen.blit(alert_text, (20, 20))  # "ALERT !!!" at top
    screen.blit(time_text, (20, 150))    # Time text below Profile text
    screen.blit(profile_text, (20, 100))  # Profile text below "ALERT !!!"
    screen.blit(field_text, (20, 200))   # Field Value text below Time text

    # Update display
    pygame.display.update()

# Stop the sound when the program quits
alert_sound.stop()

pygame.quit()

