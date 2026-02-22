#!/usr/bin/env python3
"""Remove background from hero-visual.png and scale to 130%."""
from PIL import Image
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
IMG_PATH = os.path.join(PROJECT_ROOT, "frontend", "public", "hero-visual.png")

def main():
    img = Image.open(IMG_PATH).convert("RGBA")
    data = img.getdata()
    
    # Make dark/black pixels transparent (threshold: pixels with low luminance)
    threshold = 60  # R+G+B below this -> transparent (higher = more aggressive)
    soft = 90       # Smooth transition zone
    
    new_data = []
    for item in data:
        r, g, b, a = item
        lum = r + g + b
        if lum < threshold:
            new_data.append((r, g, b, 0))
        elif lum < soft:
            # Smooth edge to avoid harsh borders
            alpha = int(255 * (lum - threshold) / (soft - threshold))
            new_data.append((r, g, b, min(255, alpha)))
        else:
            new_data.append(item)
    
    img.putdata(new_data)
    
    # Scale to 130%
    w, h = img.size
    new_w = int(w * 1.3)
    new_h = int(h * 1.3)
    img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
    
    img.save(IMG_PATH, "PNG", optimize=True)
    print(f"Done: {IMG_PATH} ({new_w}x{new_h}, background removed)")

if __name__ == "__main__":
    main()
