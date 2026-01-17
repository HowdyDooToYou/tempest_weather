from pathlib import Path
import streamlit as st


def apply_styles():
    css_path = Path(__file__).resolve().parent / "styles.css"
    try:
        css = css_path.read_text(encoding="utf-8")
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    except Exception:
        pass
