import os, sys
import re
from datetime import datetime

from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import ContentFormat
import pypdf
from azure.storage.blob import BlobServiceClient

load_dotenv()

BORDER_SYMBOL = "|"
endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")

# Sample credentials, replace with your own.
endpoint = "https://edelweissprocessor.cognitiveservices.azure.com/"
key = "379e246e59b84d94ac1f4d8f8538bdc7"

AZURE_ACCOUNT_NAME = "prodpublic24"
AZURE_ACCOUNT_KEY = "9uBBrUvKWddmweMD7uNvZb2KjaqYL1xM7I8+2M3tsVBDZZtPlbmm3cVzqIH6ZsjWaZabjVF1NJtS+AStzgxShg=="
# AZURE_CONTAINER_NAME = "brsr-fy24-2"
AZURE_CONTAINER_NAME = "ar-fy24-4"
# AZURE_CONTAINER_NAME = "ar-fy24-3"

#account_name = "prodpublic24"
    #account_key = "9uBBrUvKWddmweMD7uNvZb2KjaqYL1xM7I8+2M3tsVBDZZtPlbmm3cVzqIH6ZsjWaZabjVF1NJtS+AStzgxShg=="

def log_processing_status(file_name, status):
    """
    Logs the processing status of a file in the status.txt file.

    Args:
        file_name (str): Name of the file being processed.
        status (str): Status of the file processing ('started' or 'completed').
    """
    status_file = "fy_22_23_status.txt"
    with open(status_file, "a+") as log_file:
        log_file.write(f"{datetime.now()} - {file_name}: {status}\n")


def split_ocr_pagewise(ocr_content):
    # Attempt to split using PageFooter
    pages = re.split(r'<!-- PageFooter="[^"]+" -->', ocr_content)

    # Check if the last page is an empty string or matches the pattern for PageNumber
    if len(pages[-1].strip()) == 0 or re.match(r'^\s*<!-- PageNumber="\d+" -->\s*$', pages[-1].strip()):
        pages = pages[:-1]

    # If the pages split by PageFooter are less than 10, split using PageHeader
    if len(pages) < 10:
        pages = re.split(r'<!-- PageHeader="[^"]+" -->', ocr_content)
        if len(pages[-1].strip()) == 0 or re.match(r'^\s*<!-- PageNumber="\d+" -->\s*$', pages[-1].strip()):
            pages = pages[:-1]

    print("Total Pages", len(pages))
    return {i + 1: page.strip() for i, page in enumerate(pages) if page.strip()}
def split_pdf(input_file, output_prefix, chunk_size=150):
    """Splits a PDF file into smaller chunks.

    Args:
        input_file: Path to the input PDF file.
        output_prefix: Prefix for the output file names.
        chunk_size: Number of pages per chunk.
    """
    file_names = []
    print(input_file)
    with open(input_file, "rb") as pdf_file:
        reader = pypdf.PdfReader(pdf_file)
        num_pages = len(reader.pages)

        for i in range(0, num_pages, chunk_size):
            start_page = i
            end_page = min(i + chunk_size, num_pages)

            writer = pypdf.PdfWriter()
            for page_num in range(start_page, end_page):
                writer.add_page(reader.pages[page_num])

            output_filename = f"{output_prefix}_{start_page + 1}-{end_page}.pdf"
            file_names.append(output_filename)
            with open(output_filename, "wb") as output_file:
                writer.write(output_file)
    return file_names


def merge_horizontal_tables(md_table_1, md_table_2):
    """
    Merge two consecutive horizontal markdown tables into one markdown table.

    Args:
        md_table_1: markdown table 1
        md_table_2: markdown table 2

    Returns:
        string: merged markdown table
    """
    rows1 = md_table_1.strip().splitlines()
    rows2 = md_table_2.strip().splitlines()

    merged_rows = []
    for row1, row2 in zip(rows1, rows2):
        merged_row = (
                (row1[:-1] if row1.endswith(BORDER_SYMBOL) else row1)
                + BORDER_SYMBOL
                + (row2[1:] if row2.startswith(BORDER_SYMBOL) else row2)
        )
        merged_rows.append(merged_row)

    merged_table = "\n".join(merged_rows)
    return merged_table

def remove_header_from_markdown_table(markdown_table) :
    """
    If an actual table is distributed into two pages vertically. From analysis result, it will be generated as two tables in markdown format.
    Before merging them into one table, it need to be removed the markdown table-header format string. This function implement that.

    Args:
        markdown_table: the markdown table string which need to be removed the markdown table-header.
    Returns:
        string: the markdown table string without table-header.
    """
    HEADER_SEPARATOR_CELL_CONTENT = " - "

    result = ""
    lines = markdown_table.splitlines()
    for line in lines:
        border_list = line.split(HEADER_SEPARATOR_CELL_CONTENT)
        border_set = set(border_list)
        if len(border_set) == 1 and border_set.pop() == BORDER_SYMBOL:
            continue
        else:
            result += f"{line}\n"

    return result

def get_table_page_numbers(table):
    """
    Returns a list of page numbers where the table appears.

    Args:
        table: The table object.

    Returns:
        A list of page numbers where the table appears.
    """
    return [region.page_number for region in table.bounding_regions]


def get_table_span_offsets(table):
    """
    Calculates the minimum and maximum offsets of a table's spans.

    Args:
        table (Table): The table object containing spans.

    Returns:
        tuple: A tuple containing the minimum and maximum offsets of the table's spans.
               If the tuple is (-1, -1), it means the table's spans is empty.
    """
    if table.spans:
        min_offset = table.spans[0].offset
        max_offset = table.spans[0].offset + table.spans[0].length

        for span in table.spans:
            if span.offset < min_offset:
                min_offset = span.offset
            if span.offset + span.length > max_offset:
                max_offset = span.offset + span.length

        return min_offset, max_offset
    else:
        return -1, -1

def merge_vertical_tables(md_table_1, md_table_2):
    """
    Merge two consecutive vertical markdown tables into one markdown table.

    Args:
        md_table_1: markdown table 1
        md_table_2: markdown table 2

    Returns:
        string: merged markdown table
    """
    table2_without_header = remove_header_from_markdown_table(md_table_2)
    rows1 = md_table_1.strip().splitlines()
    rows2 = table2_without_header.strip().splitlines()

    print(rows1)
    print(rows2)

    if rows1 == [] or rows2 == []:
        return table2_without_header

    num_columns1 = len(rows1[0].split(BORDER_SYMBOL)) - 2
    num_columns2 = len(rows2[0].split(BORDER_SYMBOL)) - 2

    if num_columns1 != num_columns2:
        return table2_without_header
        # raise ValueError("Different count of columns")

    merged_rows = rows1 + rows2
    merged_table = '\n'.join(merged_rows)

    return merged_table


def merge_markdown_files(file_list):
    """Merges multiple markdown files into one.

    Args:
        file_list: List of markdown file paths.

    Returns:
        str: Merged markdown content.
    """
    merged_content = ""
    for file_name in file_list:
        with open(file_name, "r") as file:
            merged_content += file.read() + "\n"
    return merged_content


def get_merge_table_candidates_and_table_integral_span(tables):
    """
    Find the merge table candidates and calculate the integral span of each table based on the given list of tables.

    Parameters:
    tables (list): A list of tables.

    Returns:
    list: A list of merge table candidates, where each candidate is a dictionary with keys:
          - pre_table_idx: The index of the first candidate table to be merged (the other table to be merged is the next one).
          - start: The start offset of the 2nd candidate table.
          - end: The end offset of the 1st candidate table.

    list: A concision list of result.tables. The significance is to store the calculated data to avoid repeated calculations in subsequent reference.
    """


    if tables is None:
        tables = []
    table_integral_span_list = []
    merge_tables_candidates = []
    pre_table_idx = -1
    pre_table_page = -1
    pre_max_offset = 0

    for table_idx, table in enumerate(tables):
        min_offset, max_offset = get_table_span_offsets(table)
        if min_offset > -1 and max_offset > -1:
            table_page = min(get_table_page_numbers(table))
            print(f"Table {table_idx} has offset range: {min_offset} - {max_offset} on page {table_page}")

            # If there is a table on the next page, it is a candidate for merging with the previous table.
            if table_page == pre_table_page + 1:
                pre_table = {
                    "pre_table_idx": pre_table_idx,
                    "start": pre_max_offset,
                    "end": min_offset,
                    "min_offset": min_offset,
                    "max_offset": max_offset,
                }
                merge_tables_candidates.append(pre_table)

            table_integral_span_list.append(
                {
                    "idx": table_idx,
                    "min_offset": min_offset,
                    "max_offset": max_offset,
                }
            )

            pre_table_idx = table_idx
            pre_table_page = table_page
            pre_max_offset = max_offset
        else:
            print(f"Table {table_idx} is empty")
            table_integral_span_list.append(
                {"idx": {table_idx}, "min_offset": -1, "max_offset": -1}
            )

    return merge_tables_candidates, table_integral_span_list


def check_paragraph_presence(paragraphs, start, end):
    """
    Checks if there is a paragraph within the specified range that is not a page header, page footer, or page number. If this were the case, the table would not be a merge table candidate.

    Args:
        paragraphs (list): List of paragraphs to check.
        start (int): Start offset of the range.
        end (int): End offset of the range.

    Returns:
        bool: True if a paragraph is found within the range that meets the conditions, False otherwise.
    """
    for paragraph in paragraphs:
        for span in paragraph.spans:
            if span.offset > start and span.offset < end:
                # The logic role of a parapgaph is used to idenfiy if it's page header, page footer, page number, title, section heading, etc. Learn more: https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/concept-layout?view=doc-intel-4.0.0#document-layout-analysis
                if not hasattr(paragraph, 'role'):
                    return True
                elif hasattr(paragraph, 'role') and paragraph.role not in ["pageHeader", "pageFooter", "pageNumber"]:
                    return True
    return False


def check_tables_are_horizontal_distribution(result, pre_table_idx):
    """
    Identify two consecutive pages whether is horizontal distribution.

    Args:
         result: the analysis result from document intelligence service.
         pre_table_idx: previous table's index

    Returns:
         bool: the two table are horizontal distribution or not.
    """
    INDEX_OF_X_LEFT_TOP = 0
    INDEX_OF_X_LEFT_BOTTOM = 6
    INDEX_OF_X_RIGHT_TOP = 2
    INDEX_OF_X_RIGHT_BOTTOM = 4

    # For these threshold rate, could be adjusted based on different document's table layout.
    # When debugging document instance, it's better to print the actual cover rate until the two horizontal candiacate tables are merged.
    THRESHOLD_RATE_OF_RIGHT_COVER = 0.99
    THRESHOLD_RATE_OF_LEFT_COVER = 0.01

    is_right_covered = False
    is_left_covered = False

    if (
            result.tables[pre_table_idx].row_count
            == result.tables[pre_table_idx + 1].row_count
    ):
        for region in result.tables[pre_table_idx].bounding_regions:
            page_width = result.pages[region.page_number - 1].width
            x_right = max(
                region.polygon[INDEX_OF_X_RIGHT_TOP],
                region.polygon[INDEX_OF_X_RIGHT_BOTTOM],
            )
            right_cover_rate = x_right / page_width
            if right_cover_rate > THRESHOLD_RATE_OF_RIGHT_COVER:
                is_right_covered = True
                break

        for region in result.tables[pre_table_idx + 1].bounding_regions:
            page_width = result.pages[region.page_number - 1].width
            x_left = min(
                region.polygon[INDEX_OF_X_LEFT_TOP],
                region.polygon[INDEX_OF_X_LEFT_BOTTOM],
            )
            left_cover_rate = x_left / page_width
            if left_cover_rate < THRESHOLD_RATE_OF_LEFT_COVER:
                is_left_covered = True
                break

    return is_left_covered and is_right_covered


def identify_and_merge_cross_page_tables(input_file_path, output_file_path):
    """Processes a single PDF chunk to identify and merge cross-page tables.

    Args:
        input_file_path: Path to the input PDF file chunk.
        output_file_path: Path to the output markdown file.

    Returns:
        None
    """
    print("Processing PDF chunk...",input_file_path,output_file_path)
    document_intelligence_client = DocumentIntelligenceClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key),
        headers={"x-ms-useragent": "sample-code-merge-cross-tables/1.0.0"},
    )

    with open(input_file_path, "rb") as f:
        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-layout",
            analyze_request=f,
            content_type="application/octet-stream",
            output_content_format=ContentFormat.MARKDOWN,
        )

    result = poller.result()

    merge_tables_candidates, table_integral_span_list = get_merge_table_candidates_and_table_integral_span(
        result.tables)

    SEPARATOR_LENGTH_IN_MARKDOWN_FORMAT = 2
    merged_table_list = []
    for i, merged_table in enumerate(merge_tables_candidates):
        pre_table_idx = merged_table["pre_table_idx"]
        start = merged_table["start"]
        end = merged_table["end"]
        has_paragraph = check_paragraph_presence(result.paragraphs, start, end)

        is_horizontal = check_tables_are_horizontal_distribution(result, pre_table_idx)
        is_vertical = (
                not has_paragraph and
                result.tables[pre_table_idx].column_count
                == result.tables[pre_table_idx + 1].column_count
                and table_integral_span_list[pre_table_idx + 1]["min_offset"]
                - table_integral_span_list[pre_table_idx]["max_offset"]
                <= SEPARATOR_LENGTH_IN_MARKDOWN_FORMAT
        )

        if is_vertical or is_horizontal:
            print(f"Merge table: {pre_table_idx} and {pre_table_idx + 1}")
            print("----------------------------------------")

            remark = ""
            cur_content = result.content[table_integral_span_list[pre_table_idx + 1]["min_offset"]:
                                         table_integral_span_list[pre_table_idx + 1]["max_offset"]]

            if is_horizontal:
                remark = result.content[table_integral_span_list[pre_table_idx]["max_offset"]:
                                        table_integral_span_list[pre_table_idx + 1]["min_offset"]]

            merged_list_len = len(merged_table_list)
            if merged_list_len > 0 and len(merged_table_list[-1]["table_idx_list"]) > 0 and \
                    merged_table_list[-1]["table_idx_list"][-1] == pre_table_idx:
                merged_table_list[-1]["table_idx_list"].append(pre_table_idx + 1)
                merged_table_list[-1]["offset"]["max_offset"] = table_integral_span_list[pre_table_idx + 1][
                    "max_offset"]
                if is_vertical:
                    merged_table_list[-1]["content"] = merge_vertical_tables(merged_table_list[-1]["content"],
                                                                             cur_content)
                elif is_horizontal:
                    merged_table_list[-1]["content"] = merge_horizontal_tables(merged_table_list[-1]["content"],
                                                                               cur_content)
                    merged_table_list[-1]["remark"] += remark

            else:
                pre_content = result.content[table_integral_span_list[pre_table_idx]["min_offset"]:
                                             table_integral_span_list[pre_table_idx]["max_offset"]]
                merged_table = {
                    "table_idx_list": [pre_table_idx, pre_table_idx + 1],
                    "offset": {
                        "min_offset": table_integral_span_list[pre_table_idx]["min_offset"],
                        "max_offset": table_integral_span_list[pre_table_idx + 1]["max_offset"],
                    },
                    "content": merge_vertical_tables(pre_content,
                                                     cur_content) if is_vertical else merge_horizontal_tables(
                        pre_content, cur_content),
                    "remark": remark.strip() if is_horizontal else ""
                }

                if merged_list_len <= 0:
                    merged_table_list = [merged_table]
                else:
                    merged_table_list.append(merged_table)

    print(f"Merged tables: {len(merged_table_list)}")
    print(merged_table_list)

    optimized_content = ""
    if merged_table_list:
        start_idx = 0
        for merged_table in merged_table_list:
            optimized_content += result.content[start_idx: merged_table["offset"]["min_offset"]] + merged_table[
                "content"] + merged_table["remark"]
            start_idx = merged_table["offset"]["max_offset"]

        optimized_content += result.content[start_idx:]
    else:
        optimized_content = result.content

    with open(output_file_path, "w") as file:
        file.write(optimized_content)


def upload_to_azure(file_path, file_name):
    """Uploads a file to Azure Blob Storage.

    Args:
        file_path: The local path to the file.
        file_name: The name to use for the file in Azure Blob Storage.

    Returns:
        str: The URL of the uploaded blob.
    """
    print("Uploading file...", file_path)
    print("Uploading file name...", file_name)
    try:
        file_name_ocr = f"{os.path.splitext(file_name)[0]}_ocr.md"
        blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net",
                                                credential=AZURE_ACCOUNT_KEY)
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=file_name_ocr)

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

        blob_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{file_name_ocr}"
        return blob_url
    except Exception as e:
        print(f"Failed to upload to Azure Blob Storage: {e}")
        return None


def process_large_pdf(input_file, output_prefix, chunk_size=150):
    """Splits the PDF into chunks, processes each, and merges the markdown output.

    Args:
        input_file: Path to the input PDF file.
        output_prefix: Prefix for output files.
        chunk_size: Number of pages per chunk.

    Returns:
        str: URL of the uploaded markdown file.
    """
    # Step 1: Split the PDF
    log_processing_status(input_file, "started")
    chunk_files = split_pdf(input_file, output_prefix, chunk_size)

    print("Files",chunk_files)

    # Step 2: Process each chunk and save the markdown
    markdown_files = []
    for chunk_file in chunk_files:
        output_file_path = f"{chunk_file[:-4]}.md"
        identify_and_merge_cross_page_tables(chunk_file, output_file_path)
        markdown_files.append(output_file_path)

    # Step 3: Merge all markdown outputs
    final_output = merge_markdown_files(markdown_files)

    final_output_file = f"{output_prefix}.md" #final_output_file = f"{output_prefix}_final_merged.md"
    with open(final_output_file, "w") as file:
        file.write(final_output)

    # Step 4: Upload to Azure Blob Storage
    # blob_url = upload_to_azure(final_output_file, os.path.basename(final_output_file))
    # print(f"Final merged markdown uploaded to: {blob_url}")
    log_processing_status(input_file, "uploaded to azure")

    # chunk the final output to question gen
    pages_final_output = split_ocr_pagewise(final_output)
    print("Final ocr pagewise files",len(pages_final_output.keys()))
    for page_number,content in pages_final_output.items():
        print(f"Page {page_number}: {content}")



    return "blob_url"


# if __name__ == "__main__":
#     # Get the current working directory
#     current_directory = os.getcwd()
#
#     # Check if a PDF file is provided as an argument
#     if len(sys.argv) > 1:
#         input_file_path = sys.argv[1]
#         output_prefix = input_file_path.split(".")[0]
#         # Process the provided PDF file
#         final_blob_url = process_large_pdf(input_file_path, output_prefix)
#         print(f"Final merged markdown available at: {final_blob_url}")
#     else:
#         # Process all PDF files in the current directory
#         pdf_files = [f for f in os.listdir(current_directory) if f.endswith('.pdf')]
#
#         if not pdf_files:
#             print("No PDF files found in the current directory.")
#             sys.exit(1)
#
#         for pdf_file in pdf_files:
#             input_file_path = os.path.join(current_directory, pdf_file)
#             output_prefix = os.path.splitext(pdf_file)[0]
#             final_blob_url = process_large_pdf(input_file_path, output_prefix)
#             print(f"Final merged markdown for {pdf_file} available at: {final_blob_url}")

import os
import sys

# List of specific files to process
files_to_process = [
    # "ABB.AR.SPLIT.pdf",
    # "BERGEPAINT.AR.pdf",
    # "BDL.AR.SPLIT.pdf",
    # "DELHIVERY.AR.SPLIT.pdf",
    # "DMART.AR.SPLIT.pdf",
    # "EICHERMOT.AR.SPLIT.pdf",
    # "GLENMARK.AR.SPLIT.pdf",
    # "GRASIM.AR.SPLIT.pdf",
    # "HUDCO.AR.SPLIT.pdf",
    # "IGL.AR.SPLIT.pdf",
    # "ICICIBANK.AR.SPLIT.pdf",
    # "KEI.AR.SPLIT.pdf",
    # "LUPIN.AR.SPLIT.pdf",
    # "MARUTI.AR.SPLIT.pdf",
    # "METROBRAND.AR.SPLIT.pdf",
    # "MSUMI.AR.SPLIT.pdf",
    # "NMDC.AR.SPLIT.pdf",
    # "PGHH.AR.SPLIT.pdf",
    # "PIIND.AR.SPLIT.pdf",
    # "SCHAEFFLER.AR.SPLIT.pdf",
    # "STARHEALTH.AR.SPLIT.pdf",
    # "TATACHEM.AR.SPLIT.pdf",
    # "TATAMTRDVR.AR.SPLIT.pdf",
    # "TATAPOWER.AR.SPLIT.pdf",
    # "UNOMINDA.AR.SPLIT.pdf",
    # "ZFCVINDIA.AR.SPLIT.pdf"
    "BAYERCROP.pdf"
]



import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_pdf(file_name):
    try:
        input_file_path = os.path.join(current_directory, file_name)
        output_prefix = os.path.splitext(file_name)[0]
        final_blob_url = process_large_pdf(input_file_path, output_prefix)
        print(f"Final merged markdown available at: {final_blob_url}")
        return final_blob_url
    except Exception as e:
        print(f"Failed to process {file_name}: {e}")
        return None

if __name__ == "__main__":
    folder_name = "TEST"
    current_directory = os.path.join(os.getcwd(), folder_name)

    if not os.path.exists(current_directory):
        print(f"Directory {folder_name} does not exist.")
        sys.exit(1)

    os.chdir(current_directory)
    pdf_files = sorted([f for f in os.listdir('.') if f.endswith('AR.pdf')])

    last_processed_file = "CGPOWER.AR.pdf"
    if last_processed_file in pdf_files:
        last_index = pdf_files.index(last_processed_file)
        pdf_files = pdf_files[last_index + 1:]

    if not pdf_files:
        print("No new PDF files to process.")
        sys.exit(1)

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(process_pdf, pdf): pdf for pdf in pdf_files}
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                url = future.result()
                if url:
                    print(f"Processed {file} successfully, uploaded to {url}")
                else:
                    print(f"No URL returned for {file}")
            except Exception as exc:
                print(f'{file} generated an exception: {exc}')


# if __name__ == "__main__":
#     # Get the current working directory
#     current_directory = os.getcwd()
#     # Specify the folder name you want to navigate to
#     folder_name = "AR"  # Replace with the actual folder name

#     # Construct the path to the folder
#     folder_path = os.path.join(current_directory, folder_name)

#     current_directory = folder_path

#     # Check if the folder exists
#     if os.path.exists(folder_path) and os.path.isdir(folder_path):
#         # Change the working directory to the specified folder
#         os.chdir(folder_path)
#         print(f"Changed directory to {folder_path}")
#     else:
#         print(f"Folder {folder_name} does not exist in the current directory.")
#         sys.exit(1)

#     # Check if a PDF file is provided as an argument
#     print(sys.argv)
#     if len(sys.argv) > 1:
#         input_file_path = sys.argv[1]
#         output_prefix = os.path.splitext(os.path.basename(input_file_path))[0]

#         # Check if the provided file is in the list to process
#         if os.path.basename(input_file_path) in files_to_process or True:
#             # Process the provided PDF file
#             final_blob_url = process_large_pdf(input_file_path, output_prefix)
#             print(f"Final merged markdown available at: {final_blob_url}")
#         else:
#             print(f"File {input_file_path} is not in the list of files to process.")
#     else:
#         # Process all PDF files in the current directory
#         # pdf_files = [f for f in os.listdir(current_directory) if f.endswith('.pdf')]
#         pdf_files = sorted([f for f in os.listdir(current_directory) if f.endswith('AR.pdf')])

#                 # Specify the last processed file name
#         last_processed_file = "CGPOWER.AR.pdf"  # Replace with the actual last processed file name

#         # Find the index of the last processed file in the sorted list
#         if last_processed_file in pdf_files:
#             last_index = pdf_files.index(last_processed_file)
#             # Ignore all files before and including the last processed file
#             pdf_files = pdf_files[last_index + 1:]
#         else:
#             # If the last processed file is not found, process all files
#             print(f"{last_processed_file} not found in the directory. Processing all files.")



#         if not pdf_files:
#             print("No PDF files found in the current directory.")
#             sys.exit(1)

#         for pdf_file in pdf_files:
#             if pdf_file in files_to_process or True:
#                 input_file_path = os.path.join(current_directory, pdf_file)
#                 output_prefix = os.path.splitext(pdf_file)[0]
#                 final_blob_url = process_large_pdf(input_file_path, output_prefix)
#                 print(f"Final merged markdown available at: {final_blob_url}")
#             else:
#                 print(f"Skipping {pdf_file}, not in the list of files to process.")
