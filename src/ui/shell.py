import streamlit as st


def render_shell_start():
    return


def render_left_rail(page: str, render_filters):
    with st.sidebar:
        nav_options = ["home", "trends", "compare", "data"]
        def fmt(opt):
            return opt.title()

        selection = st.radio(
            "Navigation",
            nav_options,
            index=nav_options.index(page),
            format_func=fmt,
            label_visibility="collapsed",
        )
        st.session_state.page = selection

        render_filters()


def render_header_strip(content_html: str):
    st.markdown(f"<div class='header-strip'>{content_html}</div>", unsafe_allow_html=True)


def render_main_layout():
    main_col, right_col = st.columns([3, 1], gap="large")
    return main_col, right_col
