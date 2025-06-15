import streamlit as st
import tableauserverclient as TSC
import os
import copy_workbooks_retry
import csv
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Tableau Bulk Workbook Copier", layout="centered")

st.title("📤 Tableau Workbook Bulk Copier")

# --- Input Fields ---
with st.expander("🔧 Configuration", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        server_url = st.text_input("🔗 Tableau Server URL", "https://your-server")
        token_name = st.text_input("🔑 Personal Access Token Name", type="default")
        source_project = st.text_input("📁 Source Project Name")
    with col2:
        token_secret = st.text_input("🔐 Personal Access Token Secret", type="password")
        site_id = st.text_input("🏷️ Site ID (leave blank for default site)", "")
        target_project = st.text_input("📁 Target Project Name")

with st.expander("⚙️ Advanced Settings", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        batch_size = st.number_input("📦 Batch Size", min_value=1, max_value=1000, value=50, step=10)
        retry_limit = st.number_input("🔄 Retry Limit", min_value=1, max_value=10, value=3, step=1)
    with col2:
        sleep_time = st.number_input("⏱️ Sleep Time Between Batches (seconds)", min_value=0, max_value=120, value=5, step=1)

# Workbook Selection
with st.expander("📚 Workbook Selection", expanded=True):
    copy_mode = st.radio(
        "Select Copy Mode",
        ["Copy All Workbooks", "Copy Specific Workbooks"],
        horizontal=True
    )

    selected_workbooks = []
    if copy_mode == "Copy Specific Workbooks":
        if st.button("🔄 Refresh Workbook List"):
            st.cache_data.clear()
        
        @st.cache_data(ttl=300)  # Cache for 5 minutes
        def get_workbook_list(server, project_name):
            try:
                all_projects, _ = server.projects.get()
                project = next((p for p in all_projects if p.name == project_name), None)
                if not project:
                    return []
                
                all_workbooks, _ = server.workbooks.get()
                workbook_info = []
                for wb in all_workbooks:
                    if wb.project_id == project.id:
                        folder_path = ""
                        if wb.folder_id:
                            folder, _ = server.folders.get_by_id(wb.folder_id)
                            folder_path = folder.path
                        workbook_info.append({
                            "name": wb.name,
                            "folder": folder_path,
                            "display": f"{folder_path}/{wb.name}" if folder_path else wb.name
                        })
                return workbook_info
            except Exception as e:
                st.error(f"Error fetching workbooks: {e}")
                return []

        if all([server_url, token_name, token_secret, source_project]):
            try:
                auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
                server = TSC.Server(server_url, use_server_version=True)
                with server.auth.sign_in(auth):
                    workbook_list = get_workbook_list(server, source_project)
                    if workbook_list:
                        # Sort by folder path and then by workbook name
                        workbook_list.sort(key=lambda x: (x["folder"], x["name"]))
                        
                        selected_workbooks = st.multiselect(
                            "Select Workbooks to Copy",
                            options=[wb["name"] for wb in workbook_list],
                            format_func=lambda x: next((wb["display"] for wb in workbook_list if wb["name"] == x), x),
                            help="Select one or more workbooks to copy. Workbooks are grouped by folder."
                        )
                    else:
                        st.warning("No workbooks found in the source project")
            except Exception as e:
                st.error(f"Error connecting to Tableau Server: {e}")

# Action buttons
col1, col2 = st.columns(2)
with col1:
    run = st.button("🚀 Start Copying", type="primary")
with col2:
    retry = st.button("🔁 Retry Failed Workbooks", type="secondary")

# Progress tracking
progress_bar = st.progress(0)
status_text = st.empty()
verification_results = st.empty()

def update_progress(current, total):
    """Update progress bar and status text"""
    progress = current / total
    progress_bar.progress(progress)
    status_text.text(f"Processing workbook {current} of {total}")

def show_verification_results(success, results):
    """Display verification results"""
    with verification_results.container():
        st.subheader("🔍 Verification Results")
        if success:
            st.success("✅ All workbooks verified successfully!")
        else:
            st.warning("⚠️ Some workbooks failed verification")
        
        # Create a DataFrame for better display
        df = pd.DataFrame(results)
        st.dataframe(df)
        
        # Show summary
        success_count = sum(1 for r in results if r["success"])
        total_count = len(results)
        st.metric("Verification Success Rate", f"{success_count}/{total_count}")

if run or retry:
    if not all([server_url, token_name, token_secret, source_project, target_project]):
        st.error("Please fill in all required fields.")
    elif copy_mode == "Copy Specific Workbooks" and not selected_workbooks:
        st.error("Please select at least one workbook to copy.")
    else:
        try:
            with st.spinner("Signing into Tableau Server..."):
                auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
                server = TSC.Server(server_url, use_server_version=True)
                server.auth.sign_in(auth)

            st.success("✅ Signed in successfully!")
            
            # Update configuration
            copy_workbooks_retry.BATCH_SIZE = batch_size
            copy_workbooks_retry.SLEEP_TIME = sleep_time
            copy_workbooks_retry.RETRY_LIMIT = retry_limit

            if run:
                copy_workbooks_retry.init_logs()
                st.info("Starting copy process...")
                
                if copy_mode == "Copy All Workbooks":
                    copy_workbooks_retry.copy_workbooks(
                        server, 
                        source_project, 
                        target_project,
                        progress_callback=update_progress,
                        verification_callback=show_verification_results
                    )
                else:  # Copy Specific Workbooks
                    copy_workbooks_retry.copy_specific_workbooks(
                        server,
                        selected_workbooks,
                        source_project,
                        target_project,
                        progress_callback=update_progress,
                        verification_callback=show_verification_results
                    )
                st.success("🎉 All workbooks copied!")

            elif retry:
                st.info("Retrying failed workbooks...")
                copy_workbooks_retry.retry_failed_workbooks(server, source_project, target_project)
                st.success("🔁 Retry attempt complete!")

            # Display statistics
            st.subheader("📊 Operation Statistics")
            col1, col2 = st.columns(2)
            
            with col1:
                if os.path.exists(copy_workbooks_retry.SUCCESS_LOG):
                    success_df = pd.read_csv(copy_workbooks_retry.SUCCESS_LOG)
                    st.metric("Successful Copies", len(success_df))
                    if not success_df.empty:
                        st.write("Average workbook size:", f"{success_df['Size (KB)'].mean():.2f} KB")
            
            with col2:
                if os.path.exists(copy_workbooks_retry.ERROR_LOG):
                    error_df = pd.read_csv(copy_workbooks_retry.ERROR_LOG)
                    st.metric("Failed Copies", len(error_df))

            # Download buttons
            st.subheader("📥 Download Logs")
            col1, col2 = st.columns(2)
            with col1:
                if os.path.exists(copy_workbooks_retry.SUCCESS_LOG):
                    st.download_button(
                        "Download Success Log",
                        open(copy_workbooks_retry.SUCCESS_LOG, "rb"),
                        file_name=f"success_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
            with col2:
                if os.path.exists(copy_workbooks_retry.ERROR_LOG):
                    st.download_button(
                        "Download Error Log",
                        open(copy_workbooks_retry.ERROR_LOG, "rb"),
                        file_name=f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )

            # Display failed workbooks
            if os.path.exists(copy_workbooks_retry.ERROR_LOG):
                st.subheader("⚠️ Failed Workbooks")
                error_df = pd.read_csv(copy_workbooks_retry.ERROR_LOG)
                if not error_df.empty:
                    st.dataframe(error_df)
                else:
                    st.success("No failed workbooks!")

        except copy_workbooks_retry.TableauCopyError as e:
            st.error(f"❌ Error: {e}")
        except Exception as e:
            st.error(f"❌ Unexpected error: {e}")
            st.error("Please check the logs for more details.")
