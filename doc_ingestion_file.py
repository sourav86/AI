import os
import zipfile
import ntpath
import pickle
from dateutil import parser
import extract_msg
import pandas as pd
import docx
import PyPDF2
import json

PROCESSED_IDENTIFIER = '_KGSC_EXTRACT'

def list_files(folder_name):
    file_list = []
    for path, subdirs, files in os.walk(folder_name):
        for name in files:
            file_list.append(os.path.join(path, name))
    return file_list

def get_files_list(folder_name, ignore_identifier):
    file_list = list_files(source_dir)
    updated_file_list = [file for file in file_list
                         if not ignore_file(file, ignore_identifier)]
    return updated_file_list

def ignore_file(file, ignore_identifier):
    head, tail = ntpath.split(file)
    file_name = tail or ntpath.basename(head)
    if ignore_identifier in file_name:
        return True
    return False

def extract_attach_from__msg_files(dir_path, ignore_identifier):
    file_list = get_files_list(dir_path, ignore_identifier)
    file_list = [file for file in file_list if os.path.splitext(file)[1] == '.msg']
    files_extracted = 0
    for fp in file_list:
        base_file, ext = os.path.splitext(fp)
        if ignore_identifier not in base_file:
            files_extracted += extract_attachments_from_msg(fp)
    if files_extracted > 0:
        extract_attach_from__msg_files(dir_path, ignore_identifier)

def extract_attachments_from_msg(file_path):
    base_file, ext = os.path.splitext(file_path)
    os.rename(file_path, base_file + PROCESSED_IDENTIFIER + ext)
    file_path = base_file + PROCESSED_IDENTIFIER + ext
    msg = extract_msg.Message(file_path)
    msg_date_time = parser.parse(msg.date)  # datetime.datetime(1999, 8, 28, 0, 0)
    msg_date_time_str = msg_date_time.strftime('%Y%m%d%H%M%S')
    msg_subject = msg.subject
    target_dir = os.path.join(os.path.dirname(file_path), msg_subject[:20]
                              + "-_" + msg_date_time_str)
    if not os.path.exists(target_dir) and len(msg.attachments) > 0:
        os.makedirs(target_dir )
    for attachment in msg.attachments:
        attachment.save(customPath=target_dir)
    return len(msg.attachments)

def unzip_files(file_path):
    base_file, ext = os.path.splitext(file_path)
    os.rename(file_path, base_file + PROCESSED_IDENTIFIER + ext)
    file_path = base_file + PROCESSED_IDENTIFIER + ext
    dir_name = os.path.splitext(file_path)[0]
    os.mkdir(dir_name)
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(dir_name)

def unzip_all_zip_files(dir_path, ignore_identifier):
    file_list = get_files_list(dir_path, ignore_identifier)
    file_list = [file for file in file_list if os.path.splitext(file)[1] == '.zip']
    files_unzipped = 0
    for fp in file_list:
        base_file, ext = os.path.splitext(fp)
        if ignore_identifier not in base_file:
            unzip_files(fp)
            files_unzipped += 1
    if files_unzipped > 0:
        unzip_all_zip_files(dir_path, ignore_identifier)


def define_label_map():
    return {4:'Tech', 0:'Business', 3:'Sport', 1:'Entertainment', 2:'Politics'}

def load_model():
    label_map = define_label_map()
    model_file_path = r'C:\Users\nmadireddy1\Desktop\RC BOT\Coding\Adhoc\doc ingest\nmgram_logistic_regress.sav'
    loaded_model = pickle.load(open(model_file_path, 'rb'))
    vect_file_path = r'C:\Users\nmadireddy1\Desktop\RC BOT\Coding\Adhoc\doc ingest\nmgram_logistic_regress_vect.sav'
    vectorizer = pickle.load(open(vect_file_path, 'rb'))
    return label_map, loaded_model, vectorizer

def read_text_file(file_path):
    with open(file_path, 'r') as myfile:
        file_data = myfile.read()
    return file_data

def read_msg_file(file_path):
    f = file_path
    msg = extract_msg.Message(f)
    return msg.body

def read_xls_file(file_path):
    df = pd.read_excel(file_path)
    #df.as_matrix()
    df = df.applymap(str)
    return df.to_string()

def read_doc_file(file_path):
    #does not reflect text in tables, in headers, in footers and in foot notes
    doc = docx.Document(file_path)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)
    return ' '.join(fullText)

def read_pdf_file(file_path):
    pdf_file = open(file_path, 'rb')
    read_pdf = PyPDF2.PdfFileReader(pdf_file)
    number_of_pages = read_pdf.getNumPages()
    pdf_text = []
    for page in range(number_of_pages):
        page = read_pdf.getPage(0)
        page_content = page.extractText()
        pdf_text.append(str(page_content))
    return ' '.join(pdf_text)

def perform_ocr_on_image_file(file_path):
    txt = ""
    return txt

def classify(model, vectorizer, file_text, label_map, file_path, classify_data):
    test_input = vectorizer.transform([file_text])
    model_predict = model.predict(test_input)
    path, file_name = os.path.split(file_path)
    classify_data[file_name] = { 'path': file_name,
                                 'classify_tag': label_map[int(model_predict)],
                                 'text': file_text}
    #print("------------------" + label_map[int(model_predict)] + "-----------------")
    #print(fp)
    #print("------------------" + label_map[int(model_predict)] + "-----------------")
    return classify_data

def write_output_to_file(classify_output):
    proc_results_file = 'classification_results.txt'
    with open(proc_results_file, 'w') as outfile:
        json.dump(classify_output, outfile, sort_keys=True, indent=4)

source_dir = r'C:\Users\nmadireddy1\Desktop\RC BOT\Coding\Adhoc\doc ingest\target_folder'
unzip_all_zip_files(source_dir, PROCESSED_IDENTIFIER)
extract_attach_from__msg_files(source_dir, PROCESSED_IDENTIFIER)
file_list = get_files_list(source_dir, PROCESSED_IDENTIFIER)
file_list = [file for file in file_list if os.path.splitext(file)[1] != '.zip']
label_map, model, vectorizer = load_model()

classify_data = {}
for fp in file_list:
    fp_extn = os.path.splitext(fp)[-1].lower()
    if fp_extn in ['.txt', '.dat', '.csv']:
        classify_data = classify(model, vectorizer, read_text_file(fp), label_map, fp, classify_data)
    elif fp_extn.endswith('.msg'):
        classify_data = classify(model, vectorizer, read_msg_file(fp), label_map, fp, classify_data)
    elif fp_extn in ['.xls', '.xlsx']:
        classify_data = classify(model, vectorizer, read_xls_file(fp), label_map, fp, classify_data)
    elif fp_extn in ['.doc', '.docx']:
        classify_data = classify(model, vectorizer, read_doc_file(fp), label_map, fp, classify_data)
    elif fp_extn in ['.pdf']:
        classify_data = classify(model, vectorizer, read_pdf_file(fp), label_map, fp, classify_data)
    elif fp_extn in ['.png', '.jpg', '.jpeg']:
        classify_data = classify(model, vectorizer, perform_ocr_on_image_file(fp), label_map, fp, classify_data)

write_output_to_file(classify_data)
