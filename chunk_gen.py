import json
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
import re


def detect_tables_and_split(content, context_lines=2):
    """Detect tables and split the content while keeping tables with context lines as separate chunks."""
    # Regex to detect tables, ensuring the entire table is captured
    table_pattern = r'(\|.*?\|\n(?:\|.*?\|\n)+)'
    chunks = []
    last_end = 0

    for match in re.finditer(table_pattern, content):
        # Find the start context to include lines before the table
        start_context = content.rfind('\n', 0, match.start())
        for _ in range(context_lines):
            start_context = content.rfind('\n', 0, start_context)
            if start_context == -1:
                start_context = 0
                break

        # Append content before the table as a separate chunk
        if start_context > last_end:
            chunks.append({
                "content": content[last_end:start_context].strip(),
                "is_table": False
            })

        # Append the entire table as a separate chunk
        chunks.append({
            "content": content[start_context:match.end()].strip(),
            "is_table": True
        })
        last_end = match.end()

    # Append any remaining content after the last table
    if last_end < len(content):
        chunks.append({
            "content": content[last_end:].strip(),
            "is_table": False
        })

    return chunks


# def extract_page_numbers(content):
#     """Extract page numbers from the content based on the PageNumber tag."""
#     page_numbers = []
#     for match in re.finditer(r'<!-- PageNumber="([^"]+)" -->', content):
#         page_numbers.append((match.start(), match.group(1)))
#     return page_numbers
# def extract_page_numbers(content):
#     """Extract page numbers (which can be numeric or text) from the content based on the PageNumber tag."""
#     page_numbers = []
#     pattern = r'<!-- PageNumber="([^"]+)" -->'
#     matches = list(re.finditer(pattern, content))

#     # Debug: Print all matches found
#     print(f"Found {len(matches)} page number tags.")
#     for match in matches:
#         print(f"Page number '{match.group(1)}' found at position {match.start()}.")
#         page_numbers.append((match.start(), match.group(1)))

#     return page_numbers

def extract_page_numbers(content):
    """Extract named page numbers from the content based on the PageNumber tag."""
    page_numbers = []
    pattern = r'<!-- PageNumber="([^"]+)" -->'
    matches = list(re.finditer(pattern, content))

    print(f"Found {len(matches)} page number tags.")
    for match in matches:
        print(f"Page number '{match.group(1)}' found at position {match.start()}.")
        page_numbers.append((match.start(), match.group(1)))

    return page_numbers


# def assign_page_numbers_to_chunks(chunks, page_numbers):
#     """Assign page numbers to chunks based on their positions in the text."""
#     for chunk in chunks:
#         chunk_start = chunk.get("offset_start", 0)
#         chunk_end = chunk_start + len(chunk["content"])
#         chunk_pages = []
#         for (page_start, page_number) in page_numbers:
#             if page_start <= chunk_end and (chunk_pages == [] or page_start >= chunk_start):
#                 chunk_pages.append(page_number)
#             elif page_start > chunk_end:
#                 break
#         chunk['metadata']['page_numbers'] = chunk_pages

# def assign_page_numbers_to_chunks(chunks, page_numbers):
#     """Assign page numbers to chunks based on their positions in the text."""
#     for chunk in chunks:
#         chunk_start = chunk.get("offset_start", 0)
#         chunk_end = chunk_start + len(chunk["content"])
#         chunk_pages = []

#         # Debugging: Print the chunk's position and content
#         print(f"Chunk starts at {chunk_start}, ends at {chunk_end}")

#         for (page_start, page_number) in page_numbers:
#             if page_start >= chunk_start and page_start <= chunk_end:
#                 chunk_pages.append(page_number)
#                 print(f"  Page number {page_number} (start at {page_start}) added to chunk.")
#             elif page_start > chunk_end:
#                 # Since page numbers are ordered, we can break early
#                 break

#         if not chunk_pages:
#             # If no pages were found, assign a default or unknown value
#             chunk_pages = ["Unknown"]

#         chunk['metadata']['page_numbers'] = chunk_pages
#         print(f"Assigned page numbers to chunk: {chunk_pages}")

def assign_page_numbers_to_chunks(chunks, page_numbers):
    """Assign page numbers to chunks based on their positions in the text."""
    pdf_page_counter = 1  # Start the PDF page number from 1

    for chunk in chunks:
        chunk_start = chunk.get("offset_start", 0)
        chunk_end = chunk_start + len(chunk["content"])
        chunk_named_pages = []

        for (page_start, page_number) in page_numbers:
            if page_start <= chunk_end and (len(chunk_named_pages) == 0 or page_start >= chunk_start):
                chunk_named_pages.append(page_number)
            elif page_start > chunk_end:
                break

        chunk['metadata']['page_number'] = chunk_named_pages if chunk_named_pages else ['Unknown']
        chunk['metadata']['pdf_chunk_number'] = pdf_page_counter
        pdf_page_counter += 1  # Increment for each chunk, adjust based on actual chunk pagination if needed


# Load the markdown document from the file
with open('CDSL.BRSR.md', 'r') as file:
    markdown_document = file.read()

# Extract page numbers
page_numbers = extract_page_numbers(markdown_document)

# Remove page number tags from the document
markdown_document = re.sub(r'<!-- PageNumber="[^"]+" -->', '', markdown_document)

# Define the headers to split on
headers_to_split_on = [
    ("#", "Header 1"),
    ("##", "Header 2"),
]

# Initialize the markdown splitter
markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on, strip_headers=False)

# Split the document by headers
md_header_splits = markdown_splitter.split_text(markdown_document)

# Further split the content and detect tables with context lines
processed_chunks = []

for split in md_header_splits:
    content_chunks = detect_tables_and_split(split.page_content)
    for chunk in content_chunks:
        if chunk["is_table"]:
            print("-----------------TABLE----------------")
            print(chunk["content"])
            print("-----------------TABLE----------------")
        offset_start = markdown_document.find(chunk["content"])
        processed_chunks.append({
            "content": chunk["content"],
            "metadata": {**split.metadata, "is_table": chunk["is_table"], "offset_start": offset_start},
        })

# Assign page numbers to the chunks
assign_page_numbers_to_chunks(processed_chunks, page_numbers)

# Remove the offset_start from metadata if not needed
for chunk in processed_chunks:
    chunk['metadata'].pop('offset_start', None)

# Save the processed chunks to a JSON file
with open('document_chunks.json', 'w') as json_file:
    json.dump(processed_chunks, json_file, indent=4)

print("Chunks with table detection, header splits, and page numbers created and saved to 'document_chunks.json'.")