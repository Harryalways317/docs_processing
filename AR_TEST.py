import json
import os
import re
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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
AZURE_CONTAINER_NAME = "ar-fy23"

class PDFProcessor:
    def __init__(self, endpoint, key, account_name, account_key, container_name):
        self.endpoint = endpoint
        self.key = key
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.document_intelligence_client = DocumentIntelligenceClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.key),
            headers={"x-ms-useragent": "sample-code-merge-cross-tables/1.0.0"},
        )

    def log_processing_status(self, file_name, status):
        status_file = "fy_22_23_status.txt"
        with open(status_file, "a+") as log_file:
            log_file.write(f"{datetime.now()} - {file_name}: {status}\n")

    def split_pdf(self, input_file, output_prefix, chunk_size=150):
        file_names = []
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

    def identify_and_merge_cross_page_tables(self, input_file_path, output_file_path):
        with open(input_file_path, "rb") as f:
            poller = self.document_intelligence_client.begin_analyze_document(
                "prebuilt-layout",
                analyze_request=f,
                content_type="application/octet-stream",
                output_content_format=ContentFormat.MARKDOWN,
            )

        result = poller.result()
        merge_tables_candidates, table_integral_span_list = self.get_merge_table_candidates_and_table_integral_span(result.tables)

        SEPARATOR_LENGTH_IN_MARKDOWN_FORMAT = 2
        merged_table_list = []
        for i, merged_table in enumerate(merge_tables_candidates):
            pre_table_idx = merged_table["pre_table_idx"]
            start = merged_table["start"]
            end = merged_table["end"]
            has_paragraph = self.check_paragraph_presence(result.paragraphs, start, end)

            is_horizontal = self.check_tables_are_horizontal_distribution(result, pre_table_idx)
            is_vertical = (
                    not has_paragraph and
                    result.tables[pre_table_idx].column_count
                    == result.tables[pre_table_idx + 1].column_count
                    and table_integral_span_list[pre_table_idx + 1]["min_offset"]
                    - table_integral_span_list[pre_table_idx]["max_offset"]
                    <= SEPARATOR_LENGTH_IN_MARKDOWN_FORMAT
            )

            if is_vertical or is_horizontal:
                cur_content = result.content[table_integral_span_list[pre_table_idx + 1]["min_offset"]:
                                             table_integral_span_list[pre_table_idx + 1]["max_offset"]]

                if merged_table_list and merged_table_list[-1]["table_idx_list"][-1] == pre_table_idx:
                    merged_table_list[-1]["table_idx_list"].append(pre_table_idx + 1)
                    merged_table_list[-1]["offset"]["max_offset"] = table_integral_span_list[pre_table_idx + 1]["max_offset"]
                    if is_vertical:
                        merged_table_list[-1]["content"] = self.merge_vertical_tables(merged_table_list[-1]["content"], cur_content)
                    elif is_horizontal:
                        merged_table_list[-1]["content"] = self.merge_horizontal_tables(merged_table_list[-1]["content"], cur_content)
                        merged_table_list[-1]["remark"] += result.content[table_integral_span_list[pre_table_idx]["max_offset"]:
                                                                          table_integral_span_list[pre_table_idx + 1]["min_offset"]]
                else:
                    pre_content = result.content[table_integral_span_list[pre_table_idx]["min_offset"]:
                                                 table_integral_span_list[pre_table_idx]["max_offset"]]
                    merged_table = {
                        "table_idx_list": [pre_table_idx, pre_table_idx + 1],
                        "offset": {
                            "min_offset": table_integral_span_list[pre_table_idx]["min_offset"],
                            "max_offset": table_integral_span_list[pre_table_idx + 1]["max_offset"],
                        },
                        "content": self.merge_vertical_tables(pre_content, cur_content) if is_vertical else self.merge_horizontal_tables(pre_content, cur_content),
                        "remark": result.content[table_integral_span_list[pre_table_idx]["max_offset"]:
                                                 table_integral_span_list[pre_table_idx + 1]["min_offset"]].strip() if is_horizontal else ""
                    }
                    merged_table_list.append(merged_table)

        optimized_content = ""
        if merged_table_list:
            start_idx = 0
            for merged_table in merged_table_list:
                optimized_content += result.content[start_idx: merged_table["offset"]["min_offset"]] + merged_table["content"] + merged_table["remark"]
                start_idx = merged_table["offset"]["max_offset"]
            optimized_content += result.content[start_idx:]
        else:
            optimized_content = result.content

        with open(output_file_path, "w") as file:
            file.write(optimized_content)

    def upload_to_azure(self, file_path, file_name):
        try:
            file_name_ocr = f"{os.path.splitext(file_name)[0]}_ocr.md"
            blob_service_client = BlobServiceClient(account_url=f"https://{self.account_name}.blob.core.windows.net",
                                                    credential=self.account_key)
            blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=file_name_ocr)

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)

            blob_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{file_name_ocr}"
            return blob_url
        except Exception as e:
            print(f"Failed to upload to Azure Blob Storage: {e}")
            return None

    def process_large_pdf(self, input_file, output_prefix, chunk_size=150):
        self.log_processing_status(input_file, "started")
        chunk_files = self.split_pdf(input_file, output_prefix, chunk_size)

        markdown_files = []
        for chunk_file in chunk_files:
            output_file_path = f"{chunk_file[:-4]}.md"
            self.identify_and_merge_cross_page_tables(chunk_file, output_file_path)
            markdown_files.append(output_file_path)

        final_output = self.merge_markdown_files(markdown_files)
        final_output_file = f"{output_prefix}.md"
        with open(final_output_file, "w") as file:
            file.write(final_output)

        blob_url = self.upload_to_azure(final_output_file, os.path.basename(final_output_file))
        self.log_processing_status(input_file, "uploaded to azure")

        pages_final_output = self.split_ocr_pagewise(final_output)
        output_file_path = f"{output_prefix}.json"
        with open(output_file_path, 'w') as json_file:
            json.dump(pages_final_output, json_file, indent=4)

        return blob_url

    def split_ocr_pagewise(self, ocr_content):
        pages = re.split(r'<!-- PageFooter="[^"]+" -->', ocr_content)
        if len(pages[-1].strip()) == 0 or re.match(r'^\s*<!-- PageNumber="\d+" -->\s*$', pages[-1].strip()):
            pages = pages[:-1]
        if len(pages) < 10:
            pages = re.split(r'<!-- PageHeader="[^"]+" -->', ocr_content)
            if len(pages[-1].strip()) == 0 or re.match(r'^\s*<!-- PageNumber="\d+" -->\s*$', pages[-1].strip()):
                pages = pages[:-1]
        return {i + 1: page.strip() for i, page in enumerate(pages) if page.strip()}

    def merge_markdown_files(self, file_list):
        merged_content = ""
        for file_name in file_list:
            with open(file_name, "r") as file:
                merged_content += file.read() + "\n"
        return merged_content

    def merge_vertical_tables(self, md_table_1, md_table_2):
        table2_without_header = self.remove_header_from_markdown_table(md_table_2)
        rows1 = md_table_1.strip().splitlines()
        rows2 = table2_without_header.strip().splitlines()

        if not rows1 or not rows2:
            return table2_without_header

        num_columns1 = len(rows1[0].split(BORDER_SYMBOL)) - 2
        num_columns2 = len(rows2[0].split(BORDER_SYMBOL)) - 2

        if num_columns1 != num_columns2:
            return table2_without_header

        merged_rows = rows1 + rows2
        return '\n'.join(merged_rows)

    def merge_horizontal_tables(self, md_table_1, md_table_2):
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

        return "\n".join(merged_rows)

    def remove_header_from_markdown_table(self, markdown_table):
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

    def get_merge_table_candidates_and_table_integral_span(self, tables):
        if tables is None:
            tables = []
        table_integral_span_list = []
        merge_tables_candidates = []
        pre_table_idx = -1
        pre_table_page = -1
        pre_max_offset = 0

        for table_idx, table in enumerate(tables):
            min_offset, max_offset = self.get_table_span_offsets(table)
            if min_offset > -1 and max_offset > -1:
                table_page = min(self.get_table_page_numbers(table))

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
                table_integral_span_list.append(
                    {"idx": {table_idx}, "min_offset": -1, "max_offset": -1}
                )

        return merge_tables_candidates, table_integral_span_list

    def get_table_page_numbers(self, table):
        return [region.page_number for region in table.bounding_regions]

    def get_table_span_offsets(self, table):
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

    def check_paragraph_presence(self, paragraphs, start, end):
        for paragraph in paragraphs:
            for span in paragraph.spans:
                if start < span.offset < end:
                    if not hasattr(paragraph, 'role') or paragraph.role not in ["pageHeader", "pageFooter", "pageNumber"]:
                        return True
        return False

    def check_tables_are_horizontal_distribution(self, result, pre_table_idx):
        INDEX_OF_X_LEFT_TOP = 0
        INDEX_OF_X_LEFT_BOTTOM = 6
        INDEX_OF_X_RIGHT_TOP = 2
        INDEX_OF_X_RIGHT_BOTTOM = 4

        THRESHOLD_RATE_OF_RIGHT_COVER = 0.99
        THRESHOLD_RATE_OF_LEFT_COVER = 0.01

        is_right_covered = False
        is_left_covered = False

        if result.tables[pre_table_idx].row_count == result.tables[pre_table_idx + 1].row_count:
            for region in result.tables[pre_table_idx].bounding_regions:
                page_width = result.pages[region.page_number - 1].width
                x_right = max(region.polygon[INDEX_OF_X_RIGHT_TOP], region.polygon[INDEX_OF_X_RIGHT_BOTTOM])
                right_cover_rate = x_right / page_width
                if right_cover_rate > THRESHOLD_RATE_OF_RIGHT_COVER:
                    is_right_covered = True
                    break

            for region in result.tables[pre_table_idx + 1].bounding_regions:
                page_width = result.pages[region.page_number - 1].width
                x_left = min(region.polygon[INDEX_OF_X_LEFT_TOP], region.polygon[INDEX_OF_X_LEFT_BOTTOM])
                left_cover_rate = x_left / page_width
                if left_cover_rate < THRESHOLD_RATE_OF_LEFT_COVER:
                    is_left_covered = True
                    break

        return is_left_covered and is_right_covered

def process_pdf(file_name, processor):
    try:
        input_file_path = os.path.join(current_directory, file_name)
        output_prefix = os.path.splitext(file_name)[0]
        final_blob_url = processor.process_large_pdf(input_file_path, output_prefix)
        print(f"Final merged markdown available at: {final_blob_url}")
        return final_blob_url
    except Exception as e:
        print(f"Failed to process {file_name}: {e}")
        return None

if __name__ == "__main__":
    folder_name = "ARFY23"
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

    processor = PDFProcessor(endpoint, key, AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER_NAME)

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(process_pdf, pdf, processor): pdf for pdf in pdf_files}
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
