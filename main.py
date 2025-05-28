import functions_framework
import fitz  # PyMuPDF
import io
import os
from PIL import Image
from google.cloud import storage
from google.cloud.exceptions import NotFound

# --- Configuration (Hardcoded as requested) ---
# <<< IMPORTANT: Replace with YOUR actual output bucket name >>>
OUTPUT_BUCKET_NAME = "sbi-image-sample"
# <<< IMPORTANT: Replace/confirm the desired output directory within the output bucket >>>
OUTPUT_IMAGE_DIR = "extracted-pdf-images"
# Optional: Adjust header/footer ratios for image skipping
HEADER_RATIO = 0.15
FOOTER_RATIO = 0.15
# --- End Configuration ---

# --- Function to extract images (Your provided logic) ---
def extract_images_from_pdf(pdf_stream, pdf_gcs_name, image_dir, output_bucket_obj,
                            header_ratio=0.10, footer_ratio=0.10):
    """
    Extracts images from a PDF byte stream, attempts to ignore headers/footers,
    and saves them to the specified GCS bucket object.
    (Uses RuntimeError for PyMuPDF operational errors)

    Args:
        pdf_stream: A file-like object (e.g., io.BytesIO) containing the PDF data.
        pdf_gcs_name: The original name/path of the PDF file in GCS (for naming output).
        image_dir: The target directory path within the output bucket.
        output_bucket_obj: A google.cloud.storage.Bucket object for the output.
        header_ratio: Float (0.0 to <1.0), proportion of page height to consider header.
        footer_ratio: Float (0.0 to <1.0), proportion of page height to consider footer.
    """
    # Ensure ratios are valid (optional, but good practice)
    if not (0.0 <= header_ratio < 1.0): header_ratio = 0.10
    if not (0.0 <= footer_ratio < 1.0): footer_ratio = 0.10
    if header_ratio + footer_ratio >= 1.0: print(f"Warning: header+footer ratio >= 1.0 for {pdf_gcs_name}")

    pdf_document = None # Define outside try for finally block
    try:
        pdf_document = fitz.open(stream=pdf_stream, filetype="pdf")
        image_count = 0
        skipped_header_footer = 0
        # Use only the base filename for naming conventions
        pdf_filename_base = os.path.splitext(os.path.basename(pdf_gcs_name))[0]

        print(f"Processing {pdf_document.page_count} pages in '{pdf_gcs_name}'...")

        for page_num in range(pdf_document.page_count):
            page = pdf_document[page_num]
            page_height = page.rect.height
            if page_height <= 0: # Avoid division by zero for empty pages
                 print(f"Skipping page {page_num+1} due to zero height.")
                 continue

            header_limit = page_height * header_ratio
            footer_limit = page_height * (1.0 - footer_ratio)

            image_list = []
            try:
                image_list = page.get_images(full=True)
            except Exception as img_list_err:
                 print(f"Error getting image list for page {page_num+1} in {pdf_gcs_name}: {img_list_err}")
                 continue # Skip page if image list extraction fails

            processed_xrefs = set() # Track xrefs processed on this page

            for image_index, img_info in enumerate(image_list):
                xref = img_info[0]
                if xref == 0 or xref in processed_xrefs: # Skip invalid xref=0 and duplicates on the page
                    continue

                # --- Header/Footer Skipping Logic ---
                image_rects = []
                try:
                    image_rects = page.get_image_rects(img_info, transform=False) # Use img_info directly
                except Exception as rect_err:
                     print(f"Warning: Could not get rects for image xref {xref} on page {page_num+1}: {rect_err}")
                     # Decide whether to skip or try processing anyway
                     # Let's try processing anyway unless rects are absolutely needed later

                is_potentially_header_footer = False # Default to false
                if image_rects: # Only check position if we have rectangles
                    is_potentially_header_footer = True # Assume it is until proven otherwise
                    for rect in image_rects:
                        # If *any* part of the image is outside the header/footer zones, keep it.
                        if not (rect.y1 <= header_limit or rect.y0 >= footer_limit):
                            is_potentially_header_footer = False
                            break # Found part in the main content area

                if is_potentially_header_footer and image_rects: # Skip only if we have rects and all are in skip zones
                    skipped_header_footer += 1 # Count skipped potential placements
                    processed_xrefs.add(xref)
                    # print(f"Debug: Skipping potential header/footer xref {xref} on page {page_num+1}")
                    continue
                # --- End Header/Footer Skipping ---

                processed_xrefs.add(xref) # Mark as processed for this page
                try:
                    base_image = pdf_document.extract_image(xref)
                    if not base_image or not base_image.get("image"):
                        # print(f"Debug: No image data found for xref {xref} on page {page_num+1}")
                        continue

                    image_bytes = base_image["image"]
                    image_ext = base_image.get("ext", "png") # Default to png if extension unknown

                    # Determine a safe filename
                    safe_image_ext = image_ext if image_ext in ['png', 'jpeg', 'jpg', 'gif', 'bmp'] else 'png'
                    image_name = f"{pdf_filename_base}_page_{page_num + 1}_img_{image_count + 1}.{safe_image_ext}"

                    # Use PIL to handle potential format conversions and save as JPEG
                    pil_image = Image.open(io.BytesIO(image_bytes))

                    # Convert to RGB if necessary (e.g., for RGBA, P modes which JPEG doesn't directly support)
                    if pil_image.mode in ['RGBA', 'P', 'LA']:
                        # print(f"Debug: Converting image mode {pil_image.mode} to RGB for xref {xref}")
                        # Create a white background image if RGBA or LA for transparency
                        if pil_image.mode in ['RGBA', 'LA']:
                             background = Image.new("RGB", pil_image.size, (255, 255, 255))
                             background.paste(pil_image, mask=pil_image.split()[-1]) # Paste using alpha channel as mask
                             pil_image = background
                        else: # For mode 'P' (Palette)
                             pil_image = pil_image.convert('RGB')


                    # Save the image to a buffer as JPEG
                    image_buffer = io.BytesIO()
                    pil_image.save(image_buffer, "JPEG", quality=85) # Save as JPEG, adjust quality as needed
                    image_buffer.seek(0)
                    final_image_name = f"{pdf_filename_base}_page_{page_num + 1}_img_{image_count + 1}.jpg" # Ensure jpg extension
                    final_blob_path = f"{image_dir.strip('/')}/{final_image_name}"

                    # Upload to GCS
                    blob = output_bucket_obj.blob(final_blob_path)
                    blob.upload_from_file(image_buffer, content_type="image/jpeg")
                    # print(f"Debug: Saved {final_blob_path}")
                    image_count += 1

                except Exception as e:
                    print(f"Error extracting/saving image xref {xref} from page {page_num+1} in {pdf_gcs_name}: {e}")
                    # Optionally add more specific error handling for PIL, GCS uploads etc.
                    continue # Continue with the next image on the page

        print(f"Extracted {image_count} images from {pdf_gcs_name} and saved to GCS: gs://{output_bucket_obj.name}/{image_dir}")
        if skipped_header_footer > 0: print(f"Skipped {skipped_header_footer} potential header/footer image placements.")

    except RuntimeError as fe: # Catch RuntimeError, which PyMuPDF often raises for operational errors
        print(f"RuntimeError processing PDF {pdf_gcs_name} (likely PyMuPDF/Fitz error, e.g., damaged PDF): {fe}")
    except Exception as e:
        # Catch any other unexpected errors (like network issues, PIL errors, etc.)
        print(f"General error processing PDF {pdf_gcs_name}: {type(e).__name__} - {e}")
    finally:
        # Ensure the fitz document object is closed if it was successfully opened
        if pdf_document:
            try:
                pdf_document.close()
                # print(f"Debug: Closed PDF document object for {pdf_gcs_name}")
            except Exception as close_e:
                print(f"Error closing PDF document {pdf_gcs_name}: {close_e}")


# --- Cloud Function Trigger Function ---
# Entry Point: Set this name ("extract_images_from_pdf_gcs_trigger") when deploying the function
@functions_framework.cloud_event
def extract_images_from_pdf_gcs_trigger(cloud_event):
    """
    Cloud Function triggered by a GCS event (file upload/update).
    Downloads the PDF, extracts images using extract_images_from_pdf,
    and uploads them to the configured output bucket.
    """
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"] # e.g., google.cloud.storage.object.v1.finalized

    try:
        input_bucket_name = data["bucket"]
        input_file_name = data["name"]
        metageneration = data["metageneration"]
        timeCreated = data["timeCreated"]
        updated = data["updated"]

        print(f"Event ID: {event_id}")
        print(f"Event type: {event_type}")
        print(f"Input Bucket: {input_bucket_name}")
        print(f"File: {input_file_name}")
        print(f"Metageneration: {metageneration}")
        print(f"Created: {timeCreated}")
        print(f"Updated: {updated}")

        # --- Basic Validation ---
        # 1. Check if it's a PDF file based on extension
        if not input_file_name or not input_file_name.lower().endswith(".pdf"):
            print(f"Ignoring non-PDF file or file with no name: '{input_file_name}'")
            return # Not an error, just skipping

        # 2. Avoid infinite loops: Check if the input bucket is the same as the output bucket
        if input_bucket_name == OUTPUT_BUCKET_NAME:
             # And check if the file might be within the output directory prefix
             # This is a basic check, might need refinement depending on naming patterns
             if input_file_name.startswith(OUTPUT_IMAGE_DIR.strip('/') + '/'):
                  print(f"Ignoring file '{input_file_name}' as it appears to be in the output directory within the same bucket.")
                  return

        # --- Initialize GCS Client and Buckets ---
        storage_client = storage.Client()
        input_bucket = storage_client.bucket(input_bucket_name)
        # Get the output bucket using the hardcoded name
        output_bucket_obj = storage_client.get_bucket(OUTPUT_BUCKET_NAME) # Raises NotFound if it doesn't exist

        # --- Download and Process ---
        pdf_stream = None
        try:
            print(f"Attempting to process PDF: gs://{input_bucket_name}/{input_file_name}")
            blob = input_bucket.blob(input_file_name)

            # Download the PDF content as bytes
            # Increase timeout if dealing with very large PDFs or slow network
            pdf_bytes = blob.download_as_bytes(timeout=300)
            pdf_stream = io.BytesIO(pdf_bytes)

            # Call the actual image extraction logic
            extract_images_from_pdf(
                pdf_stream=pdf_stream,
                pdf_gcs_name=input_file_name,
                image_dir=OUTPUT_IMAGE_DIR,
                output_bucket_obj=output_bucket_obj, # Pass the bucket object
                header_ratio=HEADER_RATIO,
                footer_ratio=FOOTER_RATIO
            )

            print(f"Successfully finished processing trigger for: gs://{input_bucket_name}/{input_file_name}")

        except NotFound:
            print(f"Error: File not found in input bucket during download: gs://{input_bucket_name}/{input_file_name}")
        except Exception as e:
            print(f"!!! Critical failure processing {input_file_name}: {type(e).__name__} - {e}")
            # Re-raise the exception if you want the function execution to be marked as failed
            # raise e
        finally:
            # Ensure the BytesIO stream is closed
            if pdf_stream and not pdf_stream.closed:
                pdf_stream.close()
                # print(f"Debug: Closed PDF download stream for {input_file_name}")

    except KeyError as ke:
         print(f"Error: Missing expected data in CloudEvent payload. Key: {ke}. Event data: {data}")
    except NotFound as nfe:
         print(f"Error: GCS Bucket not found. Input: '{data.get('bucket', 'N/A')}', Configured Output: '{OUTPUT_BUCKET_NAME}'. Details: {nfe}")
    except Exception as e:
        print(f"An unexpected error occurred in the main trigger function: {type(e).__name__} - {e}")
        # Depending on the error, you might want to raise it to signal failure
        # raise e
