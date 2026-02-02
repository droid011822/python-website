import streamlit as st
import os
import time
from multiprocessing import Pool, cpu_count
from moviepy.editor import VideoFileClip, concatenate_videoclips, vfx
from moviepy.video.fx.gaussian_blur import gaussian_blur


# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="Distributed Video Renderer",
    page_icon="🎬",
    layout="centered"
)

UPLOAD_DIR = "uploads"
SEG_DIR = "output/segments"
PROC_DIR = "output/processed"
FINAL_DIR = "output"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(SEG_DIR, exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

# ---------------- UI ----------------
st.title("🎬 Real-Time Distributed Video Rendering")
st.caption(
    "Hackathon Demo | Parallel Video Processing using Python Multiprocessing + MoviePy"
)

st.markdown("""
**Concept**
- Split a video into 10-second chunks
- Apply filters in **parallel**
- Merge chunks back
- Compare **Sequential vs Parallel performance**
""")

uploaded = st.file_uploader("📤 Upload a video (≤ 60 seconds recommended)", type=["mp4"])

filter_type = st.selectbox(
    "🎨 Choose a filter",
    ["Grayscale", "Blur"]
)

# ---------------- CORE FUNCTIONS ----------------
def split_video(video_path, segment_length=10):
    clip = VideoFileClip(video_path)
    duration = int(clip.duration)
    segments = []

    for start in range(0, duration, segment_length):
        end = min(start + segment_length, duration)
        seg = clip.subclip(start, end)
        name = f"{SEG_DIR}/seg_{start}.mp4"
        seg.write_videofile(name, codec="libx264", audio=False, logger=None)
        segments.append(name)

    clip.close()
    return segments

def apply_filter(path):
    clip = VideoFileClip(path)

    if filter_type == "Grayscale":
        processed = clip.fx(vfx.blackwhite)
    else:
      processed = clip.fx(gaussian_blur, sigma=3)



    out = path.replace("segments", "processed")
    processed.write_videofile(out, codec="libx264", audio=False, logger=None)
    clip.close()
    return out

def sequential_render(segments):
    start = time.time()
    outputs = [apply_filter(s) for s in segments]
    return outputs, time.time() - start

def parallel_render(segments):
    start = time.time()
    with Pool(cpu_count()) as pool:
        outputs = pool.map(apply_filter, segments)
    return outputs, time.time() - start

def merge_videos(videos, out_path):
    clips = [VideoFileClip(v) for v in videos]
    final = concatenate_videoclips(clips)
    final.write_videofile(out_path, codec="libx264", logger=None)
    for c in clips:
        c.close()

# ---------------- EXECUTION ----------------
if uploaded:
    video_path = os.path.join(UPLOAD_DIR, uploaded.name)
    with open(video_path, "wb") as f:
        f.write(uploaded.read())

    st.video(video_path)

    if st.button("🚀 Run Rendering Comparison"):
        with st.spinner("Splitting video into 10-second segments..."):
            segments = split_video(video_path)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🐢 Sequential Rendering")
            seq_out, seq_time = sequential_render(segments)
            merge_videos(seq_out, f"{FINAL_DIR}/final_sequential.mp4")
            st.metric("Time Taken", f"{seq_time:.2f} sec")

        with col2:
            st.subheader("⚡ Parallel Rendering")
            par_out, par_time = parallel_render(segments)
            merge_videos(par_out, f"{FINAL_DIR}/final_parallel.mp4")
            st.metric("Time Taken", f"{par_time:.2f} sec")

        speedup = seq_time / par_time
        st.success(f"🚀 Speedup Achieved: **{speedup:.2f}x faster**")

        st.markdown("---")
        st.subheader("🎥 Output Comparison")

        st.video(f"{FINAL_DIR}/final_sequential.mp4")
        st.video(f"{FINAL_DIR}/final_parallel.mp4")
