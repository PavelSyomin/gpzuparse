import datetime
import json
import mimetypes
import pathlib

import pandas as pd

from fastapi import FastAPI, Path, Request, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from parser import Parser


folders = {
    "devplans": "media/devplans",
    "images": "media/images",
    "cache": "cache",
    "templates": "templates",
    "tmp": "tmp"
}

app = FastAPI()
templates = Jinja2Templates(directory=folders["templates"])
app.mount("/templates", StaticFiles(directory="templates"), name="templates")


for folder in folders.values():
    path = pathlib.Path(folder)
    if not path.exists():
        path.mkdir(parents=True)

@app.get("/", response_class=HTMLResponse)
@app.post("/", response_class=HTMLResponse)
async def root(request: Request):
    files = get_files()
    return templates.TemplateResponse("main.html", {"request": request, "data": files})

@app.get("/view")
async def view(request: Request, file_name: str):
    data = get_file_urls(file_name)
    return templates.TemplateResponse("view.html", {"request": request, "data": data})

@app.get("/parse")
async def parse(request: Request, file_name: str):
    parse_and_save_file(file_name, file_type="json")
    return RedirectResponse(f"/view?file_name={file_name}", status_code=302)

@app.get("/download")
async def download(request: Request, file_name: str, file_type: str):
    out_path = parse_and_save_file(file_name, file_type)
    media_type = mimetypes.types_map[f".{file_type}"]

    if out_path:
        path = pathlib.Path(out_path)
        if not path.is_file():
            return None

        filename = path.name
        return FileResponse(path=out_path, media_type=media_type, filename=filename)

@app.get("/delete")
async def delete_file(request: Request, file_name: str):
    path = pathlib.Path(folders["devplans"]) / f"{file_name}.pdf"
    if path.is_file():
        path.unlink()

    return RedirectResponse("/", status_code=302)

@app.get("/devplans")
async def devplans():
    path = pathlib.Path(folders["devplans"])
    files = path.glob(r"[RР]*.pdf")
    result = [f.name for f in files]

    return result

@app.get("/devplans/{plan_id}/status")
async def devplan_status(
    plan_id: str = Path(title="The ID of the development plan to parse (file name without extansion, e. g. RU77105000-047176-ГПЗУ")
):
    plan_file = plan_id + ".pdf"
    status = get_file_status(plan_file)
    return {"status": status}

@app.get("/devplans/{plan_id}/json")
async def devplan_json(
    plan_id: str = Path(title="The ID of the development plan to parse (file name without extansion, e. g. RU77105000-047176-ГПЗУ")
):
    parser = Parser()

    pdf_path = pathlib.Path(folders["devplans"]) / f"{plan_id}.pdf"
    if not pdf_path.is_file():
        return {"status": "Error", "message": "File not found"}

    try:
        parser.load_pdf(str(pdf_path))
    except Exception as e:
        return {
            "status": "Error",
            "message": "File exists but cannot be loaded",
            "details": str(e)
        }

    try:
        parser.parse()
    except Exception as e:
        return {
            "status": "Error",
            "message": "Cannot parse file",
            "details": str(e)
        }

    try:
        result = parser.get_result()
    except:
        return {
            "status": "Error",
            "message": "Cannot load result from parser"
        }

    return {
        "status": "OK",
        "message": f"Development plan {plan_id} has been parsed succcessfully",
        "data": result
    }

@app.get("/devplans/{plan_id}/xlsx")
async def devplan_excel(plan_id):
    out_path = parse_and_save_file(plan_id, "xlsx")
    media_type = mimetypes.types_map[f".xlsx"]

    if out_path:
        path = pathlib.Path(out_path)
        if not path.is_file():
            return None

        filename = path.name
        return FileResponse(path=out_path, media_type=media_type, filename=filename)

@app.post("/devplans/")
async def create_file(file: UploadFile):
    filename = file.filename
    content_type = file.content_type

    if content_type != mimetypes.types_map[".pdf"]:
        return {
            "status": "Error",
            "message": "Looks like that you have uploaded non-PDF file"
        }

    path = pathlib.Path(folders["devplans"]) / filename
    with open(path, "wb") as f:
        f.write(file.file.read())

    return RedirectResponse("/", status_code=302)

def get_files():
    path = pathlib.Path(folders["devplans"])
    files = path.glob(r"[RР]*.pdf")

    result = [
        {
            "name": f.name,
            "date": get_date(f.stat().st_ctime),
            "status": get_file_status(f.name),
            "urls": get_file_urls(f.name),
        }
        for f in files
    ]
    print(result)
    return result

def get_date(timestamp):
    if not timestamp:
        return ""

    dt = datetime.datetime.fromtimestamp(timestamp)
    date = dt.date()
    date_str = date.strftime("%d.%m.%Y")

    return date_str

def get_file_status(file_path):
    cache_path = pathlib.Path(folders["cache"])
    file_path = file_path.replace(".pdf", ".dump")
    pdf_file =  pathlib.Path(file_path)
    cache_file = cache_path / pdf_file.name

    if cache_file.exists():
        return "parsed"
    else:
        return "not_parsed"


def get_file_urls(file_name):
    if not file_name:
        return ""
    if "pdf" not in file_name:
        name = file_name
    else:
        name, _ = file_name.rsplit(".", maxsplit=1)

    return {
        "view": f"view?file_name={name}",
        "parse": f"parse?file_name={name}",
        "download": {
            "json": f"download?file_name={name}&file_type=json",
            "xlsx": f"download?file_name={name}&file_type=xlsx",
        },
        "delete": f"delete?file_name={name}",
    }

def parse_and_save_file(file_name, file_type="json"):
    parser = Parser()
    pdf_path = pathlib.Path(folders["devplans"]) / f"{file_name}.pdf"

    if not pdf_path.is_file():
        return None
    print(pdf_path)
    try:
        parser.load_pdf(str(pdf_path))
        parser.parse()
        result = parser.get_result()
    except Exception as e:
        print(e)
        return None
    print(len(result))
    if file_type == "json":
        out_path = save_json(file_name, result)
    elif file_type == "xlsx":
        out_path = save_excel(file_name, result)

    return str(out_path)

def save_json(file_name, result):
    tmp_dir = pathlib.Path(folders["tmp"])
    if not tmp_dir.exists():
        tmp_dir.mkdir(parents=True)

    ts = datetime.datetime.now().isoformat()
    out_file = f"{file_name}_{ts}.json"
    out_path = tmp_dir / out_file

    with open(out_path, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent="\t")

    return out_path

def save_excel(file_name, result):
    tmp_dir = pathlib.Path(folders["tmp"])
    if not tmp_dir.exists():
        tmp_dir.mkdir(parents=True)

    ts = datetime.datetime.now().isoformat()
    out_file = f"{file_name}_{ts}.xlsx"
    out_path = tmp_dir / out_file

    df = pd.json_normalize(result, sep=" / ")

    colnames = []
    for i, el in enumerate(df.iloc[0, :].items()):
        if type(el[1]) is list:
            colnames.append(el[0])

    df = df.explode(colnames)

    dfs = []
    for i, el in enumerate(df.iloc[0, :].items()):
        if type(el[1]) is dict:
            dfs.append(df.iloc[:, i].apply(pd.Series).rename(columns=lambda x: f"{el[0]} / {x}"))
        else:
            dfs.append(df.iloc[:, i])

    final_df = pd.concat(dfs, axis=1)

    final_df.to_excel(str(out_path))

    return out_path
