#!/bin/bash

# Use the first argument as input, or default to input.mp4
INPUT=${1:-"input.mp4"}
MP4_OUT="output.mp4"
GIF_PALETTE="palette.png"
GIF_OUT="output.gif"

# Ensure ffmpeg is available
if ! command -v ffmpeg &> /dev/null; then
    echo "❌ Error: ffmpeg must be installed."
    exit 1
fi

if [[ ! -f "$INPUT" ]]; then
    echo "❌ Error: File '$INPUT' not found."
    exit 1
fi

start_size=$(stat -c%s "$INPUT" 2>/dev/null || stat -f%z "$INPUT")
start_time=$SECONDS

echo "🚀 Step 1: Compressing MP4 (High Quality Text Preset)..."
# -crf 24: Higher quality than before
# -preset slow: Good balance
ffmpeg -y -i "$INPUT" \
    -vcodec libx264 -crf 24 -preset slow \
    -pix_fmt yuv420p -an "$MP4_OUT" &> /dev/null
echo "   -> Created $MP4_OUT ($(du -sh "$MP4_OUT" | cut -f1))"

echo "🎞️ Step 2: Generating High-Fidelity Palette (256 Colors)..."
# Using 256 colors ensures all syntax highlighting is preserved
# flags=neighbor: Vital for terminal text to avoid blurriness
ffmpeg -y -i "$MP4_OUT" \
    -vf "fps=12,scale=1024:-1:flags=neighbor,palettegen" \
    "$GIF_PALETTE" &> /dev/null
echo "   -> Created $GIF_PALETTE"

echo "🧪 Step 3: High-Quality GIF Conversion..."
# We map the high-quality palette back to the video
ffmpeg -y -i "$MP4_OUT" -i "$GIF_PALETTE" \
    -filter_complex "fps=12,scale=1024:-1:flags=neighbor[x];[x][1:v]paletteuse=dither=none" \
    "$GIF_OUT" &> /dev/null
echo "   -> Created $GIF_OUT ($(du -sh "$GIF_OUT" | cut -f1))"

# Calculate savings logic
mp4_size=$(stat -c%s "$MP4_OUT" 2>/dev/null || stat -f%z "$MP4_OUT")
gif_size=$(stat -c%s "$GIF_OUT" 2>/dev/null || stat -f%z "$GIF_OUT")
duration=$(( SECONDS - start_time ))

mp4_saved_pct=$(awk "BEGIN {printf \"%.2f\", (1 - $mp4_size / $start_size) * 100}")
gif_saved_pct=$(awk "BEGIN {printf \"%.2f\", (1 - $gif_size / $start_size) * 100}")

echo -e "\n---------------------------------------"
echo -e "✅ Compression Complete in ${duration}s"
echo -e "---------------------------------------"
printf "%-15s %-10s %-10s\n" "STAGE" "SIZE" "SAVINGS"
echo "---------------------------------------"
printf "%-15s %-10s %-10s\n" "Original" "$(du -sh "$INPUT" | cut -f1)" "-"
printf "%-15s %-10s %-10s\n" "Step 1: MP4" "$(du -sh "$MP4_OUT" | cut -f1)" "${mp4_saved_pct}%"
printf "%-15s %-10s %-10s\n" "Final GIF" "$(du -sh "$GIF_OUT" | cut -f1)" "${gif_saved_pct}%"
echo "---------------------------------------"

# Cleanup
rm "$GIF_PALETTE"
