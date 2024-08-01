import streamlit as st
import os
import codecs
import markdown
from decouple import config
import glob
from icecream import ic

page_title = "File Browser"
st.title(page_title)
st.session_state.current_page = page_title

if 'previous_page' not in st.session_state:
    st.session_state.previous_page = None

glob_pattern = config('RAW_DATA', default='none')

# Function to load and convert markdown to html
def load_markdown_file(markdown_file):
    with codecs.open(markdown_file, "r", encoding="utf-8") as input_file:
        text = input_file.read()
    return markdown.markdown(text)

# List of markdown files in directory
markdown_files = [f for f in glob.glob(glob_pattern, recursive=True)]

# Dropdown to select markdown file
selected_markdown_file = st.selectbox('Select a Markdown File', markdown_files)

# Load and render selected markdown file
markdown_path = selected_markdown_file
markdown_html = load_markdown_file(markdown_path)
st.markdown(markdown_html, unsafe_allow_html=True)

