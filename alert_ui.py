import pygame
from pygame.locals import QUIT

def get_ui(region_top_signals):

    for k,v in region_top_signals.items():
            print(k)
            for signal in v:
                print(f'\t{signal}')
            print()

    return None