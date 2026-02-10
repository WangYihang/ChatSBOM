#!/bin/bash

# Use the first argument as input, or default to input.mp4
INPUT=${1:-"input.mp4"}
MP4_OUT="output.mp4"
GIF_PALETTE="palette.png"
GIF_RAW="raw_conversion.gif"
GIF_OUT="output.gif"

# Ensure tools are available
if ! command -v ffmpeg &> /dev/null || ! command -v gifsicle &> /dev/null; then
    echo "❌ Error: ffmpeg and gifsicle must be installed."
    exit 1
fi

if [[ ! -f "$INPUT" ]]; then
    echo "❌ Error: File '$INPUT' not found."
    exit 1
fi

start_size=$(stat -c%s "$INPUT" 2>/dev/null || stat -f%z "$INPUT")
start_time=$SECONDS

echo "🚀 Step 1: Compressing MP4 (H.264 VerySlow/CRF)..."
ffmpeg -y -i "$INPUT" \
    -vcodec libx264 -crf 30 -preset veryslow \
    -pix_fmt yuv420p -an "$MP4_OUT" &> /dev/null
echo "   -> Created $MP4_OUT ($(du -sh "$MP4_OUT" | cut -f1))"

echo "🎞️ Step 2: Generating 16-color Palette..."
# We create a specific color map to keep the GIF crisp but tiny
ffmpeg -y -i "$MP4_OUT" \
    -vf "fps=5,scale=800:-1:flags=lanczos,palettegen=max_colors=16" \
    "$GIF_PALETTE" &> /dev/null
echo "   -> Created $GIF_PALETTE"

echo "🧪 Step 3: Initial GIF Conversion (using palette)..."
# Applying the palette to convert MP4 to a standard GIF
ffmpeg -y -i "$MP4_OUT" -i "$GIF_PALETTE" \
    -filter_complex "fps=5,scale=800:-1:flags=lanczos[x];[x][1:v]paletteuse" \
    "$GIF_RAW" &> /dev/null
echo "   -> Created $GIF_RAW ($(du -sh "$GIF_RAW" | cut -f1))"

echo "💎 Step 4: Deep Optimization with Gifsicle..."
# This step does the heavy lifting: frame deduplication and lossy compression
gifsicle -O3 --lossy=100 --colors 16 "$GIF_RAW" -o "$GIF_OUT"
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
printf "%-15s %-10s %-10s\n" "Step 3: Raw GIF" "$(du -sh "$GIF_RAW" | cut -f1)" "-"
printf "%-15s %-10s %-10s\n" "Step 4: Opt GIF" "$(du -sh "$GIF_OUT" | cut -f1)" "${gif_saved_pct}%"
echo "---------------------------------------"

# Optional: Cleanup intermediate files
# rm "$GIF_PALETTE" "$GIF_RAW"
