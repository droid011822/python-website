import streamlit as st
import os
import time
from multiprocessing import Pool, cpu_count
from moviepy.editor import VideoFileClip, concatenate_videoclips, vfx

st.set_page_config(page_title="Distributed Video Renderer", layout="centered")

st.title("🎬 Real-Time Distributed Video Rendering")
st.write("Parallel video processing using Python multiprocessing")

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def split_video(video_path, segment_length=10):
    clip = VideoFileClip(video_path)
    duration = int(clip.duration)
    os.makedirs("output/segments", exist_ok=True)
    segments = []

    for start in range(0, duration, segment_length):
        end = min(start + segment_length, duration)
        segment = clip.subclip(start, end)
        filename = f"output/segments/segment_{start}.mp4"
        segment.write_videofile(filename, codec="libx264", audio=False, logger=None)
        segments.append(filename)

    clip.close()
    return segments

def apply_filter(path):
    clip = VideoFileClip(path)
    processed = clip.fx(vfx.blackwhite)
    os.makedirs("output/processed", exist_ok=True)
    out = path.replace("segments", "processed")
    processed.write_videofile(out, codec="libx264", audio=False, logger=None)
    clip.close()
    return out

def parallel_render(segments):
    start = time.time()
    with Pool(cpu_count()) as pool:
        outputs = pool.map(apply_filter, segments)
    return outputs, time.time() - start

def merge_videos(videos):
    clips = [VideoFileClip(v) for v in videos]
    final = concatenate_videoclips(clips)
    final.write_videofile("output/final_parallel.mp4", codec="libx264", logger=None)
    for c in clips:
        c.close()

uploaded = st.file_uploader("Upload a video (MP4)", type=["mp4"])

if uploaded:
    video_path = os.path.join(UPLOAD_DIR, uploaded.name)
    with open(video_path, "wb") as f:
        f.write(uploaded.read())

    st.video(video_path)

    if st.button("🚀 Render in Parallel"):
        with st.spinner("Processing video in parallel..."):
            segments = split_video(video_path)
            outputs, time_taken = parallel_render(segments)
            merge_videos(outputs)

        st.success(f"Rendering done in {time_taken:.2f} seconds 🚀")
        st.video("output/final_parallel.mp4")
