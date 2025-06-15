import tableauserverclient as TSC
import os
import time
import csv
import logging
from datetime import datetime

# ------------------ CONFIG ------------------
TEMP_DIR = "temp_workbooks"
SUCCESS_LOG = "success_log.csv"
ERROR_LOG = "error_log.csv"
RETRY_LIMIT = 3
BATCH_SIZE = 50
SLEEP_TIME = 5
# --------------------------------------------

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tableau_copy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TableauCopyError(Exception):
    """Custom exception for Tableau copy operations"""
    pass

def init_logs():
    """Initialize log files and ensure temp directory exists"""
    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        with open(SUCCESS_LOG, 'w', newline='') as f:
            csv.writer(f).writerow(["Workbook Name", "Folder Path", "New Name", "Status", "Timestamp", "Size (KB)"])
        with open(ERROR_LOG, 'w', newline='') as f:
            csv.writer(f).writerow(["Workbook Name", "Folder Path", "Error", "Timestamp", "Attempt"])
        logger.info("Log files initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize logs: {e}")
        raise TableauCopyError(f"Failed to initialize logs: {e}")

def log_success(name, folder_path, new_name, file_path=None):
    """Log successful workbook copy with additional metadata"""
    try:
        size_kb = os.path.getsize(file_path) / 1024 if file_path and os.path.exists(file_path) else 0
        with open(SUCCESS_LOG, 'a', newline='') as f:
            csv.writer(f).writerow([name, folder_path, new_name, "Success", timestamp(), f"{size_kb:.2f}"])
        logger.info(f"Successfully copied workbook: {folder_path}/{name} -> {new_name}")
    except Exception as e:
        logger.error(f"Failed to log success for {name}: {e}")

def log_error(name, folder_path, error, attempt=1):
    """Log error with attempt count"""
    try:
        with open(ERROR_LOG, 'a', newline='') as f:
            csv.writer(f).writerow([name, folder_path, str(error), timestamp(), attempt])
        logger.error(f"Error copying workbook {folder_path}/{name} (Attempt {attempt}): {error}")
    except Exception as e:
        logger.error(f"Failed to log error for {name}: {e}")

def timestamp():
    """Get current timestamp in a consistent format"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_project(server, project_name):
    """Get project by name with proper error handling"""
    try:
        all_projects, _ = server.projects.get()
        project = next((p for p in all_projects if p.name == project_name), None)
        if not project:
            raise TableauCopyError(f"Project '{project_name}' not found")
        return project
    except Exception as e:
        logger.error(f"Error getting project {project_name}: {e}")
        raise TableauCopyError(f"Failed to get project {project_name}: {e}")

def ensure_folder_exists(server, project_id, folder_path):
    """Ensure folder exists in the project, create if it doesn't"""
    try:
        if not folder_path:
            return None

        # Split path into parts
        parts = folder_path.strip('/').split('/')
        current_path = ""
        parent_id = None

        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            
            # Check if folder exists
            folders, _ = server.folders.get()
            folder = next((f for f in folders if f.name == part and f.parent_id == parent_id), None)
            
            if not folder:
                # Create folder
                new_folder = TSC.FolderItem(name=part, parent_id=parent_id)
                folder = server.folders.create(new_folder)
                logger.info(f"Created folder: {current_path}")
            
            parent_id = folder.id

        return parent_id
    except Exception as e:
        logger.error(f"Error ensuring folder exists {folder_path}: {e}")
        raise TableauCopyError(f"Failed to create folder {folder_path}: {e}")

def get_workbook_folder(server, workbook_id):
    """Get the folder path for a workbook"""
    try:
        workbook, _ = server.workbooks.get_by_id(workbook_id)
        if workbook.folder_id:
            folder, _ = server.folders.get_by_id(workbook.folder_id)
            return folder.path
        return ""
    except Exception as e:
        logger.error(f"Error getting folder for workbook {workbook_id}: {e}")
        return ""

def verify_workbook_copy(server, source_wb, target_proj_id, target_folder_id=None):
    """Verify if a workbook was copied successfully"""
    try:
        # Get all workbooks in target project
        all_workbooks, _ = server.workbooks.get()
        target_wbs = [wb for wb in all_workbooks if wb.project_id == target_proj_id]
        
        # Find the copied workbook
        expected_name = f"{source_wb.name} - Copy"
        target_wb = next((wb for wb in target_wbs if wb.name == expected_name), None)
        
        if not target_wb:
            return False, f"Workbook {expected_name} not found in target project"
        
        # Verify folder location
        if target_folder_id:
            if target_wb.folder_id != target_folder_id:
                return False, f"Workbook {expected_name} is in wrong folder"
        
        # Verify workbook content (basic check)
        try:
            server.workbooks.download(target_wb.id, filepath=os.path.join(TEMP_DIR, "verify_temp"), include_extract=True)
            os.remove(os.path.join(TEMP_DIR, "verify_temp"))
            return True, "Verification successful"
        except Exception as e:
            return False, f"Failed to download copied workbook: {e}"
            
    except Exception as e:
        return False, f"Verification error: {e}"

def verify_batch_copy(server, source_wbs, target_proj_id, target_folder_ids):
    """Verify all workbooks in a batch were copied successfully"""
    verification_results = []
    all_successful = True
    
    for wb, folder_id in zip(source_wbs, target_folder_ids):
        success, message = verify_workbook_copy(server, wb, target_proj_id, folder_id)
        verification_results.append({
            "workbook": wb.name,
            "success": success,
            "message": message
        })
        if not success:
            all_successful = False
            logger.error(f"Verification failed for {wb.name}: {message}")
    
    return all_successful, verification_results

def copy_workbooks(server, source_project_name, target_project_name, progress_callback=None, verification_callback=None):
    """Copy workbooks with progress tracking and better error handling"""
    try:
        source_proj = get_project(server, source_project_name)
        target_proj = get_project(server, target_project_name)

        all_workbooks, _ = server.workbooks.get()
        source_wbs = [wb for wb in all_workbooks if wb.project_id == source_proj.id]
        total_workbooks = len(source_wbs)
        logger.info(f"Found {total_workbooks} workbooks to copy")

        for i in range(0, total_workbooks, BATCH_SIZE):
            batch = source_wbs[i:i+BATCH_SIZE]
            logger.info(f"Processing batch {i//BATCH_SIZE+1} ({len(batch)} workbooks)")
            
            # Store folder IDs for verification
            batch_folder_ids = []

            for wb in batch:
                attempts = 0
                while attempts < RETRY_LIMIT:
                    try:
                        folder_path = get_workbook_folder(server, wb.id)
                        logger.info(f"Downloading: {folder_path}/{wb.name}")
                        
                        file_path = server.workbooks.download(
                            wb.id, 
                            filepath=os.path.join(TEMP_DIR, wb.name), 
                            include_extract=True
                        )

                        # Ensure target folder exists
                        target_folder_id = ensure_folder_exists(server, target_proj.id, folder_path)
                        batch_folder_ids.append(target_folder_id)

                        new_name = f"{wb.name} - Copy"
                        new_wb = TSC.WorkbookItem(
                            name=new_name,
                            project_id=target_proj.id,
                            folder_id=target_folder_id
                        )
                        server.workbooks.publish(new_wb, file_path, mode=TSC.Server.PublishMode.CreateNew)
                        logger.info(f"Copied: {folder_path}/{new_name}")

                        log_success(wb.name, folder_path, new_name, file_path)
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        
                        if progress_callback:
                            progress_callback(i + 1, total_workbooks)
                        break
                    except Exception as e:
                        attempts += 1
                        logger.warning(f"Attempt {attempts} failed for {wb.name}: {e}")
                        if attempts >= RETRY_LIMIT:
                            log_error(wb.name, folder_path, e, attempts)
                            if progress_callback:
                                progress_callback(i + 1, total_workbooks)

            # Verify batch copy
            logger.info("Verifying batch copy...")
            batch_success, verification_results = verify_batch_copy(server, batch, target_proj.id, batch_folder_ids)
            
            if verification_callback:
                verification_callback(batch_success, verification_results)
            
            if not batch_success:
                logger.warning("Batch verification found issues. Check verification results for details.")
            
            logger.info(f"Sleeping {SLEEP_TIME}s between batches...")
            time.sleep(SLEEP_TIME)

    except Exception as e:
        logger.error(f"Fatal error in copy_workbooks: {e}")
        raise TableauCopyError(f"Failed to copy workbooks: {e}")

def copy_specific_workbooks(server, workbook_names, source_project_name, target_project_name, progress_callback=None, verification_callback=None):
    """Copy specific workbooks by name with progress tracking"""
    try:
        source_proj = get_project(server, source_project_name)
        target_proj = get_project(server, target_project_name)

        all_workbooks, _ = server.workbooks.get()
        source_wbs = [wb for wb in all_workbooks if wb.name in workbook_names and wb.project_id == source_proj.id]
        
        if not source_wbs:
            raise TableauCopyError(f"No matching workbooks found in source project")
        
        total_workbooks = len(source_wbs)
        logger.info(f"Found {total_workbooks} workbooks to copy")
        
        # Store folder IDs for verification
        batch_folder_ids = []

        for i, wb in enumerate(source_wbs, 1):
            attempts = 0
            while attempts < RETRY_LIMIT:
                try:
                    folder_path = get_workbook_folder(server, wb.id)
                    logger.info(f"Downloading: {folder_path}/{wb.name}")
                    
                    file_path = server.workbooks.download(
                        wb.id, 
                        filepath=os.path.join(TEMP_DIR, wb.name), 
                        include_extract=True
                    )

                    # Ensure target folder exists
                    target_folder_id = ensure_folder_exists(server, target_proj.id, folder_path)
                    batch_folder_ids.append(target_folder_id)

                    new_name = f"{wb.name} - Copy"
                    new_wb = TSC.WorkbookItem(
                        name=new_name,
                        project_id=target_proj.id,
                        folder_id=target_folder_id
                    )
                    server.workbooks.publish(new_wb, file_path, mode=TSC.Server.PublishMode.CreateNew)
                    logger.info(f"Copied: {folder_path}/{new_name}")

                    log_success(wb.name, folder_path, new_name, file_path)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    
                    if progress_callback:
                        progress_callback(i, total_workbooks)
                    break
                except Exception as e:
                    attempts += 1
                    logger.warning(f"Attempt {attempts} failed for {wb.name}: {e}")
                    if attempts >= RETRY_LIMIT:
                        log_error(wb.name, folder_path, e, attempts)
                        if progress_callback:
                            progress_callback(i, total_workbooks)

            logger.info(f"Sleeping {SLEEP_TIME}s between workbooks...")
            time.sleep(SLEEP_TIME)
            
        # Verify all copies
        logger.info("Verifying all copies...")
        batch_success, verification_results = verify_batch_copy(server, source_wbs, target_proj.id, batch_folder_ids)
        
        if verification_callback:
            verification_callback(batch_success, verification_results)
        
        if not batch_success:
            logger.warning("Verification found issues. Check verification results for details.")

    except Exception as e:
        logger.error(f"Fatal error in copy_specific_workbooks: {e}")
        raise TableauCopyError(f"Failed to copy specific workbooks: {e}")

def copy_single_workbook(server, workbook_name, source_project_name, target_project_name):
    """Copy a single workbook with enhanced error handling"""
    try:
        source_proj = get_project(server, source_project_name)
        target_proj = get_project(server, target_project_name)

        all_workbooks, _ = server.workbooks.get()
        wb = next((w for w in all_workbooks if w.name == workbook_name and w.project_id == source_proj.id), None)
        if not wb:
            raise TableauCopyError(f"Workbook '{workbook_name}' not found in source project")

        folder_path = get_workbook_folder(server, wb.id)
        logger.info(f"Retrying: {folder_path}/{wb.name}")
        
        file_path = server.workbooks.download(wb.id, filepath=os.path.join(TEMP_DIR, wb.name), include_extract=True)

        # Ensure target folder exists
        target_folder_id = ensure_folder_exists(server, target_proj.id, folder_path)

        new_name = f"{wb.name} - Copy"
        new_wb = TSC.WorkbookItem(
            name=new_name,
            project_id=target_proj.id,
            folder_id=target_folder_id
        )
        server.workbooks.publish(new_wb, file_path, mode=TSC.Server.PublishMode.CreateNew)
        log_success(wb.name, folder_path, new_name, file_path)
        
        if os.path.exists(file_path):
            os.remove(file_path)

    except Exception as e:
        logger.error(f"Error copying single workbook {workbook_name}: {e}")
        raise TableauCopyError(f"Failed to copy workbook {workbook_name}: {e}")

def retry_failed_workbooks(server, source_project_name, target_project_name):
    """Retry failed workbooks with improved error handling"""
    try:
        failed_workbooks = []
        if not os.path.exists(ERROR_LOG):
            logger.warning("No error log found. Nothing to retry.")
            return

        with open(ERROR_LOG, newline='') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            failed_workbooks = [row[0] for row in reader if row]

        if not failed_workbooks:
            logger.info("No failed workbooks to retry.")
            return

        logger.info(f"Retrying {len(failed_workbooks)} failed workbooks...")

        for wb_name in failed_workbooks:
            try:
                copy_single_workbook(server, wb_name, source_project_name, target_project_name)
            except Exception as e:
                logger.error(f"Retry failed for {wb_name}: {e}")
                log_error(wb_name, "", e)

    except Exception as e:
        logger.error(f"Fatal error in retry_failed_workbooks: {e}")
        raise TableauCopyError(f"Failed to retry workbooks: {e}")

if __name__ == "__main__":
    # Replace with your actual Tableau Server config
    SERVER_URL = "https://your-server"
    TOKEN_NAME = "your-token-name"
    TOKEN_SECRET = "your-token-secret"
    SITE_ID = ""  # or "your-site-content-url"

    SOURCE_PROJECT = "Your Source Project"
    TARGET_PROJECT = "Your Target Project"

    auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, SITE_ID)
    server = TSC.Server(SERVER_URL, use_server_version=True)

    with server.auth.sign_in(auth):
        init_logs()
        copy_workbooks(server, SOURCE_PROJECT, TARGET_PROJECT)
        print("\n✅ Done with all batches.")
