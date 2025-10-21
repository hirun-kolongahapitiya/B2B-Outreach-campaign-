from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd, io, zipfile, csv
from datetime import datetime

app = FastAPI(title="Nestable Generator API")

REQUIRED_COLUMNS = ["first_name","last_name","email","role_need","company","industry","country"]

def build_sequence(fn, ln, email, role, company, industry, facts):
    q = (datetime.utcnow().month - 1)//3 + 1
    opener1 = f"{fn}, congrats on recent momentum at {company}" if facts else \
              f"{fn}, many {industry} teams speed up delivery with embedded offshore pods"
    opener2 = facts[1] if len(facts) >= 2 else \
              f"Happy to outline how embedded {role} roles slot into {company}'s sprint cadence"
    return {
        "Subject 1": f"Adding {role} without slowing {company}'s roadmap",
        "Body 1": f"{opener1}.\n\nI run Nestable.ai — we make it fast and low-friction to add vetted, full-time engineers in Sri Lanka who plug into {company}'s workflow like in-house. We handle recruiting, onboarding, payroll, compliance, and workspace.\n\nOpen to a quick call?",
        "Subject 2": f"{company} × embedded {role}: simple, managed model",
        "Body 2": f"{opener2}. Start with 1–2 engineers on a short trial. UK-hours overlap; your standups/Git flow; we manage ops & performance.\n\n15 minutes this week to compare options?",
        "Subject 3": f"Time-to-hire for {role}: avoid the 6–10 week wait",
        "Body 3": f"Many {industry} teams wait ~6–10 weeks for a strong {role}. We show 2–3 vetted profiles in days and can start a trial inside two weeks.\n\nQuick call to review profiles and timelines?",
        "Subject 4": "De-risked start: small trial, clear deliverables",
        "Body 4": "Start with 1 engineer, short trial, explicit deliverables, stop/continue decision. Keep velocity without long commitments.\n\nPencil 15 minutes to map a low-risk first step?",
        "Subject 5": f"Wrap-up: revisit in Q{q} or compare profiles now?",
        "Body 5": "If bandwidth is tight, we can time it to your next sprint. I can share anonymized profiles + day-rate ranges.\n\nIntro call this week, or should I circle back next month?"
    }

@app.get("/healthz")
def health():
    return {"ok": True}

@app.post("/generate")
async def generate(file: UploadFile = File(...)):
    # Read CSV
    try:
        df = pd.read_csv(file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV read error: {e}")

    # Validate columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing columns: {missing}")

    # Dedupe by Email + Company
    df['__email_norm'] = df['email'].astype(str).str.strip().str.lower().fillna('')
    df['__company_norm'] = df['company'].astype(str).str.strip().str.lower().fillna('')
    df = df.drop_duplicates(subset=['__email_norm','__company_norm']) \
           .drop(columns=['__email_norm','__company_norm'])

    # Build sequences
    rows = []
    for _, r in df.iterrows():
        seq = build_sequence(r["first_name"], r["last_name"], r["email"],
                             r["role_need"], r["company"], r["industry"], facts=[])
        rows.append({
            "First Name": r["first_name"], "Last Name": r["last_name"], "Email": r["email"],
            "Company": r["company"], "Role Need": r["role_need"], "Industry": r["industry"], **seq
        })

    out = pd.DataFrame(rows, columns=[
        "First Name","Last Name","Email","Company","Role Need","Industry",
        "Subject 1","Body 1","Subject 2","Body 2","Subject 3","Body 3","Subject 4","Body 4","Subject 5","Body 5"
    ])

    # CSV + XLSX in memory
    csv_buf = io.StringIO(); out.to_csv(csv_buf, index=False, quoting=csv.QUOTE_MINIMAL)
    xlsx_buf = io.BytesIO(); out.to_excel(xlsx_buf, index=False); xlsx_buf.seek(0)

    # ZIP them
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("nestable_b2b_campaign.csv", csv_buf.getvalue())
        z.writestr("nestable_b2b_campaign.xlsx", xlsx_buf.getvalue())
    zip_buf.seek(0)

    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=nestable_campaign_outputs.zip"}
    )
