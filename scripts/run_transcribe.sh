#!/bin/bash

# 📂 Set your working directory
cd ~/Documents/WhisperProjects || exit

# ✅ Activate virtual environment
source ~/whisper-env/bin/activate

# 🧠 Transcribe the file (adjust filename as needed)
FILENAME="$1"
MODEL="medium"
LANGUAGE="English"
FORMAT="all"

if [ -z "$FILENAME" ]; then
  echo "⚠️  Please provide a video file path as an argument."
  echo "Usage: ./run_transcribe.sh your_video.mp4"
  exit 1
fi

# 🚀 Run Whisper transcription
whisper "$FILENAME" \
  --model "$MODEL" \
  --task transcribe \
  --language "$LANGUAGE" \
  --output_format "$FORMAT"



📌 How to Use It

#Create the script file
    nano ~/Documents/WhisperProjects/run_transcribe.sh

#Paste the script above into the terminal editor.

#Save and exit:
    #Press Control + O to save
    #Press Enter to confirm the file name
    #Press Control + X to exit

#Make it executable
    chmod +x ~/Documents/WhisperProjects/run_transcribe.sh

#Run the script
    ~/Documents/WhisperProjects/run_transcribe.sh 2025-11-03_11-07-47.mp4



