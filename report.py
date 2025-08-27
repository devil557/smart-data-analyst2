from fpdf import FPDF
from io import BytesIO


def create_pdf(query, result):
    """Generate PDF report"""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="AI Data Analysis Report", ln=True, align="C")
    pdf.ln(10)
    pdf.multi_cell(0, 10, f"Query: {query}")
    pdf.multi_cell(0, 10, f"Result: {result}")

    # Output PDF to bytes
    pdf_bytes = pdf.output(dest="S").encode("latin-1")
    pdf_output = BytesIO(pdf_bytes)

    return pdf_output.getvalue()
