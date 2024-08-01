import streamlit as st
from icecream import ic
from utils import df_to_html

page_title = "Search History"
st.title(page_title)
st.session_state.current_page = page_title

if 'previous_page' not in st.session_state:
    st.session_state.previous_page = None

history_count = 0

if 'search_history' in st.session_state:
    if st.session_state.search_history:
        for i, search in enumerate(reversed(st.session_state.search_history)):
            if search:
                with st.expander(f"Search {search['search_time']} - {search['search_type']} - '{search['search_term']}'", expanded=False):
                    st.markdown(f"**Search Time:** {search['search_time']}")
                    st.markdown(f"**Search Term:** {search['search_term']}")
                    st.markdown(f"**Search Type:** {search['search_type']}")
                    st.markdown(f"**Search Field:** {search['search_field']}")
                    st.markdown(f"**Search Display Field:** {search['search_display_field']}")
                    st.markdown("#### Hits:")
                    st.html(search['df_hits_html'])
                history_count += 1
    else:
        st.write("No search history found.")
else:
    st.write("No search history found.")
