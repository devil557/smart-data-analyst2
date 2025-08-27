# pdf_export.py
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import simpleSplit
from datetime import datetime
from io import BytesIO

def export_analysis_to_pdf(analysis_text):
    try:
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4

        # Title
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, height - 50, "AI Data Analysis Report")

        # Date
        c.setFont("Helvetica", 10)
        c.drawString(50, height - 70, f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Prepare wrapped text
        c.setFont("Helvetica", 12)
        wrapped_lines = simpleSplit(analysis_text, "Helvetica", 12, width - 100)

        # Start position for text
        x, y = 50, height - 100
        line_height = 15  # space between lines

        for line in wrapped_lines:
            if y < 50:  # If we reach bottom margin
                c.showPage()
                c.setFont("Helvetica", 12)
                x, y = 50, height - 50  # reset position on new page
            c.drawString(x, y, line)
            y -= line_height

        c.showPage()
        c.save()

        buffer.seek(0)
        return True, buffer

    except Exception as e:
        return False, str(e)
