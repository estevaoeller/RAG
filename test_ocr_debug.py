from pathlib import Path
from pdf2image import convert_from_path
import pytesseract

pdf_path = Path("/var/tmp/kb_rag_queue/10_inbox/COMPESA__CONTRATO-PPP.pdf")

print(f"[DEBUG] PDF: {pdf_path}", flush=True)

print("[DEBUG] convert_from_path iniciado", flush=True)
imagens = convert_from_path(
    str(pdf_path),
    dpi=150,
    first_page=1,
    last_page=3
)
print(f"[DEBUG] imagens geradas: {len(imagens)}", flush=True)

for i, imagem in enumerate(imagens, start=1):
    print(f"[DEBUG] OCR página {i} iniciado", flush=True)
    texto = pytesseract.image_to_string(imagem, lang="eng")
    print(f"[DEBUG] OCR página {i} concluído", flush=True)
    print(f"[DEBUG] primeiros 500 caracteres da página {i}:", flush=True)
    print(texto[:500], flush=True)
    print("-" * 80, flush=True)

print("[DEBUG] teste concluído", flush=True)
