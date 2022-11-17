import sys
import datetime
import pathlib
import pickle
import re

import numpy as np
import pandas as pd
import pymorphy2
import PyPDF2
import tabula


morph = pymorphy2.MorphAnalyzer()


class Parser():
    # Mappings for headers with respective data in format
    # {
    #    attribute name -> tuple(first line of the header (or it's part) after which the attribute value follows,
    #                            first line of the next header (the end of data reading),
    #                            starting header length (offset to start reading data),
    #                            length of the attribute content (None if not limited — then read till the first line of next header))
    # }

    HEADERS_RU = {
        "rightsholder": ("Градостроительный план земельного участка подготовлен на основании",
                         "Местонахождение земельного участка",
                         1,
                         None),
        "location": ("Местонахождение земельного участка",
                     "Описание границ земельного участка:",
                     1,
                     None),
        "cad_number": ("Кадастровый номер земельного участка",
                       "Площадь земельного участка",
                       1,
                       1),
        "area": ("Площадь земельного участка",
                 "Информация о расположенных в границах земельного участка объектах капитального строительства",
                 1,
                 1),
        "ppt_pmt": ("Реквизиты проекта планировки территории и (или) проекта межевания территории",
                    "Градостроительный план подготовлен",
                    3,
                    None),
        "usekinds": ("основные виды разрешенного использования земельного участка:",
                     "условно разрешенные виды использования земельного участка:",
                     1,
                     None),
        "capital_buildinds_presence": ("Информация о расположенных в границах земельного участка объектах капитального строительства",
                            "Информация о границах зоны планируемого размещения объекта капитального строительства",
                            1,
                            None),
        "capital_buildings_descr": ("3.1. Объекты капитального строительства",
                              "3.2. Объекты, включенные в единый государственный реестр объектов культурного наследия",
                              1,
                              None),
        "heritage": ("3.2. Объекты, включенные в единый государственный реестр объектов культурного наследия",
                     "4. Информация о расчетных показателях минимально допустимого уровня обеспеченности",
                     2,
                     None),
    }

    HEADERS_RF = {
        "rightsholder": ("Градостроительный план земельного участка подготовлен на основании обращения правообладателя",
                         "Местонахождение земельного участка",
                         3,
                         None),
        "location": ("Местонахождение земельного участка",
                     "Описание границ земельного участка (образуемого земельного участка):",
                     1,
                     None),
        "cad_number": ("Кадастровый номер земельного участка (при наличии) или в случае, предусмотренном",
                       "Площадь земельного участка",
                       4,
                       1),
        "area": ("Площадь земельного участка",
                 "Информация о расположенных в границах земельного участка объектах капитального строительства",
                 1,
                 1),
        "ppt_pmt": ("Реквизиты проекта планировки территории и (или) проекта межевания территории",
                    "Градостроительный план подготовлен",
                    3,
                    None),
        "usekinds": ("основные виды разрешенного использования земельного участка:",
                     "условно разрешенные виды использования земельного участка:",
                     1,
                     None),
        "capital_buildinds_presence": ("Информация о расположенных в границах земельного участка объектах капитального строительства",
                                    "Информация о границах зоны планируемого размещения объекта капитального строительства",
                                    1,
                                    None),
        "capital_buildings_descr": ("3.1. Объекты капитального строительства",
                              "3.2. Объекты, включенные в единый государственный реестр объектов культурного наследия",
                              1,
                              None),
        "heritage": ("3.2. Объекты, включенные в единый государственный реестр объектов культурного наследия",
                     "4. Информация о расчетных показателях минимально допустимого уровня обеспеченности",
                     2,
                     None),
    }


    def __init__(self, use_cache=True):
        self._file_path = None
        self._cache = None
        self._type = None
        self._district_data = pd.read_csv("./data/districts.csv", encoding="Windows-1251",
                                          delimiter=";")

        self._districts = self._create_distict_dict()
        self._use_cache = use_cache

    def _create_distict_dict(self):
        districts = self._district_data.loc[self._district_data["Name"].str.contains("административный округ")]
        codes = list(map(lambda x: str(x)[2:5], districts['Kod_okato']))
        return dict(zip(codes, districts["Name"]))

    def load_pdf(self, file_path):
        self._text = {}
        self._data = {}
        self._tables = []
        self._parsed = {}

        cached = self._load_from_cache(file_path)
        if cached:
            return None

        try:
            with open(file_path, "rb") as f:
                pdf_file = PyPDF2.PdfFileReader(f)
                print(f"File {file_path} loaded")
                self._npages = pdf_file.numPages
                print(f"PDF contatins {self._npages} pages")
                for page_number in range(self._npages):
                    page = pdf_file.getPage(page_number)
                    self._text[page_number + 1] = page.extractText()
                print(f"Text loaded")
        except:
            print(f"Unable to read text from PDF file {file_path}")

        try:
            print("Loading tables")
            self._tables = tabula.read_pdf(file_path, pages="all")
            print(f"Loaded {len(self._tables)} tables")
        except:
            print(f"Unable to read tables from PDF file {file_path}")

        self._data["file_path"] = file_path
        self._save_to_cache(file_path)

    def parse(self):
        # Parse number
        self._data["number"] = self._extract_number()

        # Set gpzu type ('RU', 'РФ')
        self._set_type()

        # Walk through text and extract possible raw data
        self._extract_from_text()

        # Process date-related attributes
        self._extract_dates()

        # Get data from tables
        self._extract_from_tables()

        # Postprocess (cleanup etc)
        self._postprocess()

    def _build_cache_file_path(self, file_path):
        cache_path = pathlib.Path("cache")
        file_path = file_path.replace(".pdf", ".dump")
        pdf_file =  pathlib.Path(file_path)
        cache_file = cache_path / pdf_file.name

        return cache_file

    def _load_from_cache(self, file_path):
        if not self._use_cache:
            return None

        cache_file = self._build_cache_file_path(file_path)
        if cache_file.exists():
            try:
                with open(cache_file, "rb") as f:
                    data = pickle.load(f)
                    self._tables = data["tables"]
                    self._text = data["text"]
            except:
                print(f"Cannot load cache")
                return None

            print(f"Data for file {file_path} loaded from cache")
            return True

        return False

    def _save_to_cache(self, file_path):
        cache_file = self._build_cache_file_path(file_path)

        try:
            with open(cache_file, "wb") as f:
                data = {
                    "tables": self._tables,
                    "text": self._text
                }
                pickle.dump(data, f)
        except Exception as e:
            print(f"Cannot save to cache")
            print(e)

    def get_result(self):
        if self._parsed:
            return self._parsed

        return {"status": "Error"}

    def _set_type(self):
        if self._data.get("number", "").startswith("RU"):
            self._type = "RU"
        elif self._data.get("number", "").startswith("РФ"):
            self._type = "РФ"

    def _extract_from_text(self):
        if not self._type:
            return

        if self._type == "RU":
            mapping = self.HEADERS_RU
        elif self._type == "РФ":
            mapping = self.HEADERS_RF

        lines = []
        for page in self._text.values():
            page_lines = page.split("\n")
            lines.extend(page_lines)

        start = 2
        print(len(lines), type(lines))
        for attr, position in mapping.items():
            start_header, stop_header, offset, length = position
            attr_value = self._get_attr_by_position(text=lines, start=start,
                                                                start_header=start_header, stop_header=stop_header,
                                                                offset=offset, length=length)
            self._data[attr] = attr_value

    def _get_attr_by_position(self, text=None, start=None,
                              start_header=None, stop_header=None,
                              offset=None, length=None):
        if not all((text, start, start_header, stop_header, offset)):
            return None


        # Detect attribute value start
        for i, line in enumerate(text):
            if line.startswith(start_header):
                attr_value_start = i + offset
                break
        else:
            return None

        # Read attribute value if it occupies only one line
        if length == 1:
            return text[attr_value_start]

        # Read multi-line attribute value
        value_strings = []
        for i, line in enumerate(text[attr_value_start:]):
            if line.startswith(stop_header):
                break
            value_strings.append(line)

        value = " ".join(value_strings)

        return value

    def _extract_from_tables(self):
        self._extract_from_limits_table()
        self._extract_from_unregulated_objects_table()

    def _extract_from_limits_table(self):
        if len(self._tables) == 0:
            self._data["subzones"] = {}
            return

        for table in self._tables:
            if table.columns[0].startswith("Предельные (минимальные"):
                break

        table = table.dropna(axis=1, how="all")
        for i, row in table.iterrows():
            cell1 = str(row[0])
            last_cell = str(row[-1])
            if cell1.startswith("1") and last_cell.endswith("8"):
                break
            if cell1.startswith("1") and not last_cell.endswith("8"): # wrong table
                self._data["subzones"] = {}
                return

        new_colnames = table.iloc[i, :].str.replace(" ", "_", regex=False)
        table = table.iloc[i+3:, :]
        table.rename(columns=new_colnames, inplace=True)

        if table.columns[0] not in ("1_2_3", "1_2_3_4"):
            print("Unrecognized shape for limits table")
            self._data["subzones"] = {}
            return

        subzones = self._get_subzones_from_table(table)

        subzones_data = self._extract_data_by_subzones(subzones)

        self._data["subzones"] = subzones_data

    def _extract_from_unregulated_objects_table(self):
        if len(self._tables) == 0:
            self._data["has_unregulated_objects"] = False
            return

        # Default workflow
        for i, table in enumerate(self._tables):
            columns = table.columns
            if columns[0].startswith("Причины отнесения"):
                has_data = self._check_unregulated_objects_table(table)
                break

        # If something is wrong with previous table and we have at least one more table
        if has_data is None and i+1 < len(self._tables):
            table = self._tables[i+1]
            has_data = self._check_unregulated_objects_table(table)

        if has_data is None:
            has_data = False

        self._data["has_unregulated_objects"] = has_data

    def _check_unregulated_objects_table(self, table):
        for i, row in table.iterrows():
            cell1 = str(row[0])
            if cell1.startswith("1"):
                last_cell = str(row[-1])
                # If the count of columns != 8, it's wrong table
                if not last_cell.endswith("8"):
                    return None
                break
        else:
            return None

        # If there are few rows after the row with enumeration, the table is probably empty
        thres = 3
        if table.shape[0] - i < thres:
            return False

        return True

    def _get_subzones_from_table(self, table):
        table["subzone_index"] = np.nan

        start_index = 1
        for i, row in table.iterrows():
            cell1 = str(row[0])
            if "No" in cell1 and not row[1:].any():
                table.loc[i, "subzone_index"] = start_index
                start_index += 1
        table["subzone_index"].fillna(method="ffill", inplace=True)

        if table["subzone_index"].any():
            subzones = [part[1] for part in table.groupby("subzone_index")]
        else:
            subzones = [table]

        return subzones

    def _extract_data_by_subzones(self, subzones):
        data = {}
        # print('subzones', subzones)
        for subzone in subzones:
            if subzone["subzone_index"].any():
                title_cell = str(subzone.iat[0, 0])
            else:
                title_cell = None
            print(title_cell)
            if title_cell:
                s_number = re.search(r"No (\d+)", title_cell)
                if s_number:
                    s_number = s_number.group(1)

                s_area = re.search(r"\(\d+\.\d+ ?\w+\)", title_cell)
                if s_area:
                    s_area = s_area.group(0)
                else:
                    s_area = "-"

                if "Назначение объекта" in title_cell:
                    _, s_description = title_cell.split(" - ", maxsplit=1)
                    s_description = s_description.strip()
                else:
                    s_description = "-"
            else:
                s_number = "-1"
                s_area = "-"
                s_description = "-"

            col5 = subzone["5"].str.cat()
            matches = re.findall(r"\d+", col5)
            print(matches)
            if len(matches) == 2:
                s_max_height = matches[0]
                s_max_floors = matches[1]
            elif len(matches) == 1 and "Предельная высота" in col5:
                s_max_height = matches[0]
                s_max_floors = "-"
            elif len(matches) == 1 and "Предельное количество этажей" in col5:
                s_max_height = "-"
                s_max_floors = matches[0]
            elif "Предельная высота" in col5 and "Предельное количество этажей" in col5:
                height_part, floors_part = col5.split("Предельное", maxsplit=1)
                _, s_max_height = height_part.rsplit(" - ", maxsplit=1)
                _, s_max_floors = floors_part.rsplit(" - ", maxsplit=1)
                s_max_height = s_max_height.strip()
                s_max_floors = s_max_floors.strip()
            else:
                s_max_height = "-"
                s_max_floors = "-"

            col6 = subzone["6"].str.cat()
            parts = col6.rsplit(" - ", maxsplit=1)
            if len(parts):
                s_max_dev_percent = parts[1].strip()
            else:
                s_max_dev_percent = "-"

            s_max_density_index = 0 if s_number == "-1" else 1
            s_max_density_cell = str(subzone.iat[s_max_density_index, -2])
            parts = s_max_density_cell.rsplit(" - ", maxsplit=1)
            if len(parts) == 2:
                s_max_density = parts[1]
            else:
                s_max_density = "-"

            col8 = subzone["8"].str.cat(sep=" ")
            print(col8)
            sentences = re.findall(r"[А-Я][^А-Я]+", col8)
            print(sentences)

            s_area_by_floor = dict.fromkeys(("total", "living", "nonliving", "livingspace", "builtin"), 0)
            s_area_total = dict.fromkeys(("total", "living", "nonliving", "livingspace", "builtin", "underground"), 0)
            for sentence in sentences:
                if sentence.startswith("Суммарная поэтажная площадь объекта"):
                    match = re.search(r"\d+,?\d*", sentence)
                    if match:
                        s_area_by_floor["total"] = match.group(0)

                if "Наземная площадь" in sentence or "Общая площадь" in sentence:
                    match = re.search(r"\d+,?\d*", sentence)
                    if match:
                        s_area_total["total"] = match.group(0)

                # living = re.findall('(?<=- жилая часть (кв.м) -\s)\d+', sentence)
                # if living:
                s_area_by_floor['living'] = s_area_total['total']

                notliving = re.findall('(?<=нежилая часть (кв.м) -\s)\d+', sentence)
                if notliving:
                    s_area_by_floor['nonliving'] = notliving[0]
                    if notliving[0]:
                        s_area_by_floor['living'] = s_area_total["total"] - s_area_total["nonliving"]

                livingspace = re.findall('(?<=- жилая часть (кв.м) -\s)\d+', sentence)
                if livingspace:
                    s_area_by_floor['livingspace'] = livingspace
                else:
                    s_area_by_floor['livingspace'] = 0

            data[s_number] = {
                "area": s_area,
                "description": s_description,
                "max_height": s_max_height,
                "max_floors": s_max_floors,
                "max_dev_percent": s_max_dev_percent,
                "max_density": s_max_density,
                "area_by_floor": s_area_by_floor,
                "area_total": s_area_total
            }

        return data

    def _extract_number(self):
        number = ""
        page1_strings = self._text[1].split("\n")

        for string in page1_strings:
            if string.startswith("№"):
                number = string.replace("№", "").strip()
                break

        return number

    def _postprocess(self):
        self._parsed = {}

        gpzu_basic_info = {
            "Уникальный номер записи": self._get_ids(),
            "Номер документа ГПЗУ": self._data.get("number"),
            "Дата выдачи ГПЗУ": self._format_date(self._data.get("start_date")),
            "Статус ГПЗУ": self._data.get("status"),
            "Срок действия ГПЗУ": self._format_date(self._data.get("end_date")),
            "Правообладатель или иной получатель ГПЗУ": self._postprocess_rightsholder(self._data.get("rightsholder")),
            "Тип правообладателя или получателя ГПЗУ": self._detect_rightsholder_type(self._data.get("rightsholder")),
        }
        self._parsed["Реквизиты ГПЗУ"] = gpzu_basic_info

        try:
            adm_district, settlement, address = self._postprocess_location(self._data.get("location"))
        except:
            adm_district, settlement, address = [None] * 3
        cad_number = self._postprocess_cad_number(self._data.get("cad_number"))
        ppt, pmt = self._postprocess_ppt_pmt(self._data.get("ppt_pmt"))
        gpzu_location = {
            "Административный округ": adm_district,
            "Район (поселение)": settlement,
            "Строительный адрес": address,
            "Кадастровый номер земельного участка (ЗУ) или условный номер": cad_number,
            "Наличие проекта планировки территории (ППТ) в границах ГПЗУ реквизиты документа": ppt["status"],
            "Реквизиты документа ППТ": ppt["details"],
            "Наличие отдельного проекта межевания территории в границах ГПЗУ": pmt["status"],
            "Реквизиты проекта межевания территории": pmt["details"]
        }
        self._parsed["Территория (Местоположение) земельного участка (ЗУ)"] = gpzu_location

        usekinds_group, usekinds_codes = self._postprocess_usekinds(self._data.get("usekinds"))
        usekinds = {
            "Наименование условной группы использования ЗУ по ВРИ": usekinds_group,
            "Коды основных видов разрешенного использования (ВРИ) земельного  участка (ЗУ)": usekinds_codes,
        }
        self._parsed["Виды разрешенного использования земельного участка (ВРИ ЗУ)"] = usekinds

        spatial_params = {
            "Площадь земельного  участка (ЗУ), кв.м": self._postprocess_area(),
            "Наличие подзон ЗУ, номера": self._postprocess_subzone_numbers(),
            "Площади подзон ЗУ, кв.м": self._postprocess_subzone_areas()
        }
        self._parsed["Территориальные показатели ГПЗУ"] = spatial_params

        heights, floors, dev_percent, density = self._postprocess_limits()
        limits = {
            "Высота застройки , м": heights,
            "Количество надземных этажей, шт": floors,
            "Процент застроенности, %": dev_percent,
            "Плотность застройки, тыс. кв. м/га": density
        }
        self._parsed["Предельные параметры разрешенного строительства, реконструкции объектов капитального строительства (ОКС)"] = limits

        objectives, descriptions, floor_areas, total_areas = self._postprocess_cap_params()
        cap_params = {
            "Назначение ОКС": objectives,
            "Наименование, описание ОКС": descriptions,
            "Наличие объектов на которые действие градостроительного регламента не распространяется или не устанавливается": self._postprocess_unregulated(),
            "Суммарная поэтажная площадь наземной части зданий и сооружений в габаритах наружных стен, кв.м": floor_areas,
            "Общая площадь зданий и сооружений, кв.м": total_areas,
            "Суммарная поэтажная площадь наземной части зданий и сооружений в габаритах наружных стен по всем подзонам, кв.м": self._get_sums(floor_areas),
            "Общая площадь зданий и сооружений по всем подзонам, кв.м": self._get_sums(total_areas)
        }
        self._parsed["Параметры и площади строящихся и реконструируемых объектов капитального строительства (ОКС) на ЗУ"] = cap_params

        try:
            (has_existing_caps, existing_caps_cnt, existing_caps_objective, existing_caps_descr,
                existing_caps_floors, existing_caps_area) = self._postprocess_existing_cap_params()
        except:
            (has_existing_caps, existing_caps_cnt, existing_caps_objective, existing_caps_descr,
             existing_caps_floors, existing_caps_area) = [None] * 6
        existing_cap_params = {
            "Наличие или отсутствие существующих на ЗУ ОКС": has_existing_caps,
            "Общее число существующих ОКС, единиц": existing_caps_cnt,
            "Назначение существующих ОКС": existing_caps_objective,
            "Наименование, описание существующих ОКС": existing_caps_descr,
            "Максимальное число наземных этажей существующих ОКС": existing_caps_floors,
            "Общая площадь существующих ОКС, кв.м": existing_caps_area
        }
        self._parsed["Параметры и площади существующих на ЗУ объектов капитального строительства (ОКС)"] = existing_cap_params

        try:
            has_heritage, heritage_cnt, heritage_desc, heritage_ids, heritage_regn = self._postprocess_heritage()
        except:
            has_heritage, heritage_cnt, heritage_desc, heritage_ids, heritage_regn = [None] * 5
        heritage = {
            "Наличие или отсутствие существующих на ЗУ ОКН": has_heritage,
            "Общее число существующих на ЗУ ОКН": heritage_cnt,
            "Наименование, описание ОКН": heritage_desc,
            "Идентификационный номер ОКН": heritage_ids,
            "Регистрационный номер ОКН": heritage_regn
        }
        self._parsed["Объекты,включенные в единый государственный реестр объектов культурного наследия (ОКН)"] = heritage

    def _get_ids(self):
        subzones = self._data.get("subzones")
        number = self._data.get("number")
        if not subzones:
            return number

        keys = list(subzones.keys())

        if len(keys) == 1 and keys[0] == "-1":
            return number
        else:
            return [f"{number}№{szn}" for szn in subzones]

    def _format_date(self, date):
        if type(date) is not datetime.date:
            return None

        return date.strftime("%d.%m.%Y")

    @staticmethod
    def _find_noun(words):
        nouns = list(filter(lambda x: morph.parse(x)[0].tag.POS == "NOUN", words.split()))
        if nouns:
            return nouns
        return None

    def _change_form_to_normal(self, text):
        words = text.split("\"")

        parsed_words = []

        for i, word_seq in enumerate(words):
            if i % 2:
                parsed_words.append(word_seq)
                continue

            parsed_words_ = []
            for word in word_seq.split():
                noun = self._find_noun(word_seq)
                if noun is None:
                    number, gender = None, None
                    parsed_words_.extend([morph.parse(word)[0].normal_form for word in words])
                elif len(noun) > 1:
                    parsed_words.append(word_seq)
                    break
                else:
                    noun = noun[0]
                    number, gender = morph.parse(noun)[0].tag.number, morph.parse(noun)[0].tag.gender

                parsed_word = morph.parse(word)[0]

                if parsed_word.tag.POS not in ["ADJF"]:
                    parsed_words_.append(parsed_word.normal_form)
                else:
                    try:
                        parsed_words_.append(morph.parse(parsed_word.normal_form)[0].inflect({number, gender}).word)
                    except:
                        parsed_words_.append(parsed_word.normal_form)

            parsed_words.append(" ".join(parsed_words_) + " ")

        return "\"".join(parsed_words)

    def _postprocess_rightsholder(self, rightsholder):
        if rightsholder is None:
            return None

        if self._type == "RU":
            rightsholder =  rightsholder.replace("обращения ", "")

        rightsholder = re.sub(r"от \d{2}\.\d{2}.\d{4}", "", rightsholder).strip()
        return self._change_form_to_normal(rightsholder)

    def _detect_rightsholder_type(self, rightsholder):
        if not rightsholder:
            return None

        for option in ("обществ", "товариществ", "акционер", "партнерст", "предприят", "некоммерческ",
                       "департамент", "управление", "отдел"):
            if option in rightsholder.lower():
                return "ЮЛ"

        if "индивидуальн" in rightsholder.lower():
            return "ФЛ или ИП"

        if '\"' in rightsholder:
            return "ЮЛ"

        parts = rightsholder.split(" ")

        if len(parts) < 4:
            for part in parts[:2]:
                if part.lower() == part:
                    break
            else:
                return "ФЛ или ИП"

        return "ЮЛ"

    def _postprocess_cad_number(self, cad_number):
        if not cad_number:
            return None

        return cad_number.strip()

    def _postprocess_ppt_pmt(self, ppt_pmt):
        ppt = {"status": None, "details": "-"}
        pmt = {"status": None, "details": "-"}

        if type(ppt_pmt) is not str:
            return ppt, pmt

        ppt_desc_start = ppt_pmt.find("планировк")
        pmt_desc_start = ppt_pmt.find("межеван")

        if ppt_desc_start < pmt_desc_start:
            ppt_desc = ppt_pmt[:pmt_desc_start]
            pmt_desc = ppt_pmt[pmt_desc_start:]
        else:
            ppt_desc = ppt_pmt[ppt_desc_start:]
            pmt_desc = ppt_pmt[:ppt_desc_start]

        if "не утвержд" in ppt_desc:
            ppt["status"] = "Не утвержден"
        else:
            ppt["status"] = "Утвержден"
            matches = re.findall(r"№ ?\d+[-а-яА-Я]* *от *\d{2}\.\d{2}.\d{4}", ppt_desc)
            if matches:
                ppt["details"] = "; ".join(matches)

        if "не утвержд" in pmt_desc:
            pmt["status"] = "Не утвержден"
        else:
            pmt["status"] = "Утвержден"
            matches = re.findall(r"№ ?\d+[-а-яА-Я]* от \d{2}\.\d{2}.\d{4}", pmt_desc)
            if matches:
                pmt["details"] = "; ".join(matches)

        return ppt, pmt

    @staticmethod
    def _get_address(text, settlement):
        res = re.findall(f"(?<=Почтовый адрес ориентира:\s)[0-9а-яА-Я.,\s]+", text)
        if res:
            return res
        return re.findall(f"(?<={settlement},\s)[0-9а-яА-Я.,\s]+", text)

    @staticmethod
    def _get_settlement(text):
        res = re.findall("(?<=поселение\s)[а-яА-Я]+", text)
        if res:
            return res
        return re.findall("(?<=муниципальное образование\s)[а-яА-Я]+", text)

    def _get_adm_district(self, settlement):
        settlement = morph.parse(settlement)[0].normal_form.capitalize()
        code = self._district_data.loc[self._district_data["Name"].str.contains(settlement)]["Kod_okato"].values[0]
        code = str(code)[2:5]
        return self._districts[code]

    def _postprocess_location(self, location):
        print('----------------')
        try:
            settlement = self._get_settlement(location)[0]
            address = self._get_address(location, settlement)[0]
            district = self._get_adm_district(settlement)
        except:
            settlement, address, district = None, None, None

        if type(location) is not str:
            return [None] * 3

        return [district, settlement, address ]

    def _postprocess_usekinds(self, usekinds):
        if type(usekinds) is not str:
            return None, None

        matches = re.findall(r"\([0123456789.]+\)", usekinds)
        usekind_codes = [match[1:-1] for match in matches]

        if len(usekind_codes) == 0 and "не распространяется" in usekinds or "не устанавливается" in usekinds:
            return "Нежилая", usekinds.strip()
        else:
            codes = set(usekind_codes)
            living_codes = set([code for code in usekind_codes if re.match(r"2[\d.]+", code) or code == "13.2"])
            nonliving_codes = codes - living_codes
            #print(living_codes, nonliving_codes)
            if len(living_codes) > 0 and len(nonliving_codes) > 0:
                usekind_group = "Смешанная"
            elif len(living_codes) == 0 and len(nonliving_codes) > 0:
                usekind_group = "Нежилая"
            elif len(living_codes) > 0 and len(nonliving_codes) == 0:
                usekind_group = "Жилая"
            else:
                usekind_group = "-"

        usekind_codes = "; ".join(usekind_codes)

        return usekind_group, usekind_codes

    def _postprocess_area(self):
        area = self._data.get("area")
        if type(area) is not str:
            return None

        unit = "ha" if "га" in area else "sqm"

        matches = re.findall(r"\d+", area)
        if len(matches) == 0:
            return None
        else:
            area = matches[0]

        try:
            area = int(area)
        except ValueError:
            return None

        if unit == "ha":
            area = area * 10000

        return area

    def _postprocess_subzone_numbers(self):
        subzones = self._data.get("subzones")
        if not subzones:
            return None

        if len(subzones) == 1:
            return "Нет"
        else:
            return [f"№ {szn}" for szn in subzones]

    def _postprocess_subzone_areas(self):
        subzones = self._data.get("subzones")
        if not subzones:
            return None

        if len(subzones) == 1:
            return None

        areas = []
        for subzone in subzones.values():
            sa = subzone.get("area", "-")
            if sa == "-":
                sa = None
            else:
                unit = "ha" if "га" in sa else "sqm"
                match = re.search(r"\d+", sa)
                if match:
                    sa = match.group(0)
                try:
                    sa = sa.replace(",", ".")
                    sa = float(sa)
                    if unit == "ha":
                        sa = sa * 10000
                except:
                    sa = None

            areas.append(sa)

        return areas

    def _postprocess_limits(self):
        subzones = self._data.get("subzones")
        if not subzones:
            return [None] * 4

        print(subzones)

        heights, floors, dev_percents, densities = [], [], [], []
        for szn in subzones.values():
            heights.append(szn.get("max_height"))
            floors.append(szn.get("max_floors"))
            dev_percents.append(szn.get("max_dev_percent"))
            densities.append(szn.get("max_density"))

        return heights, floors, dev_percents, densities

    def _postprocess_cap_params(self):
        subzones = self._data.get("subzones")
        if not subzones:
            return [None] * 4
        floor_area_labels = ("Всего", "Жилой застройки", "Нежилой застройки", "Жилых помещений",
                             "Встроенно-пристроенных, отдельно стоящих нежилых помещений")
        total_area_labels = ("Всего", "Жилой застройки", "Нежилой застройки", "Жилых помещений",
                             "Встроенно-пристроенных, отдельно стоящих нежилых помещений", "Подземного пространства")

        objectives, descriptions, floor_areas, total_areas = [], [], [], []
        print('ddddddddddd')

        for szn in subzones.values():
            descr = szn.get("description")
            print(descr)
            descriptions.append(descr)
            if descr:
                objective = "Жилое" if re.match("Жил(ая|ое|ой)", descr) else "Нежилое"
            else:
                objective = None
            objectives.append(objective)

            floor_area = szn.get("area_by_floor")
            print(floor_area)
            if floor_area and len(floor_area.values()) == len(floor_area_labels):
                floor_area = dict(zip(floor_area_labels, floor_area.values()))
            else:
                floor_area = dict.fromkeys(floor_area_labels, None)
            floor_areas.append(floor_area)

            total_area = szn.get("area_total")
            if total_area and len(total_area.values()) == len(total_area_labels):
                total_area = dict(zip(total_area_labels, total_area.values()))
            else:
                total_area = dict.fromkeys(total_area_labels, None)
            total_areas.append(total_area)
        print(floor_areas)
        return objectives, descriptions, floor_areas, total_areas

    def _get_sums(self, elements):
        if not elements or len(elements) == 0:
            return {}

        keys = elements[0].keys()
        result = {}

        for key in keys:
            value = 0
            for element in elements:
                val = element[key]
                try:
                    value += int(val)
                except:
                    pass
            result[key] = value

        return result

    def _postprocess_unregulated(self):
        unregulated = self._data.get("has_unregulated_objects")
        if not unregulated:
            return None

        return "Есть" if unregulated else "Нет"

    def _postprocess_existing_cap_params(self):
        text = self._data.get('capital_buildings_descr')
        param1 = "Отсутствуют" if text == "Информация отсутствует" else "Присутствуют"
        if param1 == "Отсутствуют":
            param2 = 0
            param3 = "Нет"
            param4 = "Нет"
            param5 = 0
            param6 = 0
            return [param1, param2, param3, param4, param5, param6]

        param2 = len(re.findall("№", text))
        param3 = list(filter(lambda x: x != "Нежилое", re.findall("(?<=Назначение:\s)\w+", text)))

        if len(param3) == param2:
            param3 = "Жилое"
        elif not len(param3):
            param3 = "Нежилое"
        else:
            param3 = "Смешанное"
        print(text)
        # \d\s
        param4 = ', '.join([x.strip() for x in re.findall("(?<=№\s\d\sна чертеже ГПЗУ\s)[\w\s]+(?=\sАдрес)", text)])
        print(len(param4), param4)
        if not len(param4):
            param4 = ', '.join([x.strip() for x in re.findall("(?<=№\d\s)[\w\s]+", text)])

        try:
            param5 = max(list(map(lambda x: max([int(y) for y in x.split('-')]), re.findall("(?<=Количество этажей:\s)[\d-]+", text))))
        except:
            param5 = 0

        try:
            param6 = round(sum(list(map(lambda x: float(x), re.findall("(?<=Площадь:\s)[\d.]+", text)))), 2)
        except:
            param6 = 0

        return [param1, param2, param3, param4, param5, param6]

    def _postprocess_heritage(self):
        text = self._data.get('heritage')
        param1 = "Отсутствуют" if text == "Информация отсутствует" else "Присутствуют"
        if param1 == "Отсутствуют":
            param2 = 0
            param3 = "Нет"
            param4 = "Нет"
            param5 = "Нет"
            return [param1, param2, param3, param4, param5]

        param2 = len(re.findall("№", text))
        param3 = "; ".join(re.findall("(?<=Наименование объекта:\s)[\w\s\d]+", text))
        if not param3:
            param3 = "; ".join(re.findall("(?<=Наименование ансамбля:\s)[\w\s\d]+", text))

        param4 = "; ".join(re.findall("(?<=Идентификационный номер объекта:\s)\d+", text))
        param5 = "; ".join(re.findall("(?<=Регистрационный номер объекта:\s)\d+", text))

        if not len(param3):
            param3 = "-"

        if not len(param4):
            param4 = "-"

        if not len(param5):
            param5 = "-"

        return [param1, param2, param3, param4, param5]

    def _extract_dates(self):
        self._data["start_date"] = self._extract_date()
        self._data["end_date"] = self._get_end_date()
        self._data["status"] = self._get_status()

    def _extract_date(self):
        date = None
        for page in self._text.values():
            if "Дата выдачи " in page and "Градостроительный план подготовлен" in page:
                break
        else:
            return date

        lines = page.split("\n")
        date_str = None

        for line in lines:
            if line.startswith("Дата выдачи"):
                date_str = line
                break
        else:
            return date

        date = re.search(r"\d{2}\.\d{2}.\d{4}", date_str)
        if not date:
            return

        date = date.group(0)

        date = datetime.datetime.strptime(date, "%d.%m.%Y").date()

        return date

    def _get_end_date(self):
        date = self._data.get("start_date")
        if not date:
            return

        end_date = datetime.date(date.year + 3, date.month, date.day)
        if datetime.date(2020, 4, 6) <= end_date <= datetime.date(2021, 1, 1):
            end_date += datetime.timedelta(days=365)
        elif datetime.date(2022, 4, 13) <= end_date <= datetime.date(2023, 1, 1):
            end_date += datetime.timedelta(days=365)

        return end_date

    def _get_status(self):
        if len(self._text) == 1:
            for page in self._text.values():
                if "Первом отделе" in page:
                    return "Секретно"

        end_date = self._data.get("end_date")
        if not end_date:
            return

        if end_date:
            today = datetime.date.today()
            if end_date >= today:
                return "Действует"
            else:
                return "Срок действия истек"

