import pandas as pd
import csv
from django.http import HttpResponse
from datetime import datetime
from io import BytesIO
import logging
from django.conf import settings
from django.db import models


class DataFrameGenerics:
    def __init__(self) -> None:
        pass

    def validate_columns(self, df: pd.DataFrame, target_columns: list) -> bool:
        for coluna in target_columns:
            if coluna not in df.columns:
                logging.warning(f"[validate_columns] {coluna} doesn't exist!")
                return False
        return True

    def convert_to_date(self, df: pd.DataFrame, target_columns: dict = {}) -> pd.DataFrame:
        target_columns = target_columns or {coluna: '%Y-%m-%d' for coluna in df.columns if coluna.startswith('data_')}
        if not isinstance(target_columns, dict):
            logging.warning("[convert_to_date] target_columns must be a dictionary!")
            return df

        if not self.validate_columns(df, target_columns.keys()):
            return df

        for coluna, frmt in target_columns.items():
            df[coluna] = pd.to_datetime(df[coluna].fillna(pd.NaT), format=frmt, errors='coerce').dt.date

        return df

    def convert_to_float(self, df: pd.DataFrame, target_columns: list = [], decimals: int = None) -> pd.DataFrame:
        target_columns = target_columns or [coluna for coluna in df.columns if coluna.startswith('valor_')]

        if isinstance(target_columns, str):
            target_columns = target_columns.split(",")

        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            if not pd.api.types.is_float_dtype(df[coluna]):
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('float', errors='ignore')
            if decimals:
                df[coluna] = df[coluna].round(decimals)

        return df

    def convert_to_first_month_day(self, df: pd.DataFrame, target_columns: list) -> pd.DataFrame:
        target_columns = target_columns if isinstance(target_columns, list) else target_columns.split(",")

        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            df[coluna] = pd.to_datetime(df[coluna], format='%Y-%m-%d', errors='coerce').dt.to_period('M').dt.to_timestamp()

        return df
    
    def convert_to_int(self, df: pd.DataFrame, target_columns: list) -> pd.DataFrame:
        target_columns = target_columns if isinstance(target_columns, list) else target_columns.split(",")

        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            if not pd.api.types.is_integer_dtype(df[coluna]):
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('int64', errors='ignore')

        return df

    def optimize_to_category(self, df: pd.DataFrame, target_columns: list) -> pd.DataFrame:
        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            if not pd.api.types.is_categorical_dtype(df[coluna]):
                df[coluna] = df[coluna].astype('category', errors='ignore')
        return df

    def convert_case(self, df: pd.DataFrame, mode: str, target_columns: list) -> pd.DataFrame:
        if df.empty:
            return df
        
        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            if pd.api.types.is_string_dtype(df[coluna]):
                if mode == 'upper':
                    df[coluna] = df[coluna].str.upper()
                elif mode == 'lower':
                    df[coluna] = df[coluna].str.lower()
            else:
                logging.warning(f"[convert_case] {coluna} is not of string dtype!")

        return df

    def convert_to_object(self, df: pd.DataFrame, target_columns: list) -> pd.DataFrame:
        target_columns = target_columns if isinstance(target_columns, list) else target_columns.split(",")

        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            df[coluna] = df[coluna].astype('str')

        return df

    def prepare_to_json(self, df: pd.DataFrame, target_columns: list = []) -> pd.DataFrame:
        target_columns = target_columns or [col for col in df.columns if col.startswith('id_') or col.endswith('_id')]
        if not self.validate_columns(df, target_columns):
            return df

        df[target_columns] = df[target_columns].apply(pd.to_numeric, errors="coerce").astype('Int64')
        df.loc[:, df.select_dtypes("category").columns] = df.loc[:, df.select_dtypes("category").columns].astype("object")
        df.replace({pd.NA: None}, inplace=True)

        return df

    def round_float_to_export(self, df: pd.DataFrame, target_columns: list =[], decimals: int = 2) -> pd.DataFrame:
        target_columns = target_columns or [col for col in df.columns if col.startswith('valor')]

        if not self.validate_columns(df, target_columns):
            return df

        for coluna in target_columns:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('float64').round(decimals)

        return df

    def compare_df_to_insert(self, df: pd.DataFrame, df_reference: pd.DataFrame, join_columns: dict, target_column: str, final_columns: list) -> pd.DataFrame:
        for column, datatype in join_columns.items():
            for dtf in [df, df_reference]:
                if column in dtf.columns:
                    dtf[column] = dtf[column].astype(datatype, errors='ignore')
        df = pd.merge(df, df_reference, how='left', on=list(join_columns.keys()))

        # If target column is null, the data doesn't exist in the reference table
        # Returns only new data
        has_not_db = df[target_column].isna()

        return df.loc[has_not_db, final_columns].reset_index(drop=True)

    def export_csv(self, df: pd.DataFrame, filename: str = "file", extension: str = ".csv") -> HttpResponse:
        response = HttpResponse(content_type='text/csv')
        response["Content-Disposition"] = f"attachment; filename={filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{extension}"
        df.to_csv(path_or_buf=response, sep=';', encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC, index=False)

        return response

    def load_db(self, df: pd.DataFrame, target_model: models.Model, target_columns: list = None) -> dict:
        log = {
            'success': False,
            'details': None,
            'started_at': datetime.now(),
            'finished_at': None,
            'inserted': 0
        }

        if not target_columns:
            target_columns = df.columns

        if not self.validate_columns(df, target_columns):
            log["details"] = "Algumas colunas não existem no dataframe inputado!"
            return log

        if not df.empty:
            values_list = (
                df.replace({pd.NA: None})
                  .loc[:, target_columns]
                  .reset_index(drop=True)
                  .to_dict('records')
            )

            objs = [target_model(**vals) for vals in values_list]
            try:
                target_model.objects.bulk_create(objs=objs, batch_size=500)
                log['inserted'] = len(values_list)
                log['details'] = 'Sucesso!'
                log['success'] = True
            except Exception as err:
                print(str(err))
                log['details'] = f'Falha na inserção dos dados: {str(err)}'
        else:
            log['details'] = "Dataframe vazio!"

        log['finished_at'] = datetime.now()
        return log

    def export_xlsx(self, df: pd.DataFrame, file_name: str = "file", sheet_name: str = "base", columns_formats: list = None):
        FONTE = "Calibri"
        pd.io.formats.excel.ExcelFormatter.header_style = None
        FORMATS = {
            "date": {
                "num_format": "dd/mm/yyyy"
            },
            "date_time": {
                "num_format": "dd/mm/yyyy hh:mm:ss"
            },
            "money": {
                "num_format": '''_-R$ * #,##0.00_-;-R$ * #,##0.00_-;_-R$ * "-"??_-;_-@_-'''
            },
            "code": {
                # "bold": True,
                "num_format": "##################"
            },
            "decimal": {
                "num_format": "#,##0.00"
            },
            "percent": {
                "num_format": "##0.00%"
            }
        }

        # If the columns_formats doesnt exists, it's created from the df columns:
        if not columns_formats:
            columns_formats = [dict({"column": c}) for c in df.columns]

        # If the columns_formats exists, but doesnt contains some df column, the basic format is added for it
        for column in df.columns:
            element = [c for c in columns_formats if c["column"] == column]
            if not element:
                columns_formats.append({"column": column})
        # Ensure the only the columns in the df are in the columns formats:
        columns_formats = [i for i in columns_formats if i["column"] in df.columns]

        # Add some basic formats to the columns if it doesnt exists:
        for col in columns_formats:
            if col.get("format", None):
                continue
            if col["column"].startswith(("data", "dt")):
                col.update({"format": "date"})
            elif col["column"].endswith(("_em", "_at")):
                col.update({"format": "date_time"})
            elif col["column"].startswith("id_") or col["column"].endswith("_id"):
                col.update({"format": "code"})
            elif col["column"].startswith(("valor", "vr_", "percent")):
                col.update({"format": "decimal"})

        vp6_color = '#104288'
        main_color = getattr(settings, 'CLIENT_COLORS', {}).get('bg_color', vp6_color)

        with BytesIO() as content:
            writer = pd.ExcelWriter(
                content,
                engine='xlsxwriter',
                # date_format='dd/mm/yyyy',
                # datetime_format='dd/mm/yyyy hh:mm:ss'
            )

            df.to_excel(writer, index=False, startrow=0, startcol=0, sheet_name=sheet_name)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            header_format = workbook.add_format(
                {
                    'bold': True,
                    'font_color': main_color,
                    'bottom': 5,
                    'bottom_color': main_color,
                    'font_name': FONTE,
                    'font_size': 10})

            arr = df.to_numpy()
            # Define o formato das células de acordo com as colunas:
            for col_index, col in enumerate(df.columns):

                frmt = {}
                for item in columns_formats:
                    if item["column"] == col:
                        frmt = item

                max_values_length = int(df[col].astype('str').str.len().max()) if not df.empty else 10
                max_width = max(max_values_length, len(col)) * 1.2

                # Define o formato da coluna a partir dos FORMATOS existentes
                col_format = workbook.add_format(
                    dict(
                        FORMATS.get(frmt.get("format", ""), {}),
                        **{
                            'font_name': FONTE,
                            'font_size': 9
                        }
                    )
                )

                if frmt.get("format", "").startswith('date'):
                    for row in range(0, len(arr)):
                        worksheet.write(row + 1, col_index, arr[row, col_index], col_format)
                worksheet.set_column(col_index, col_index, int(max_width), col_format)

                # Define o formato da célula do cabeçalho
                value = frmt.get('column') if not frmt.get('rename', None) else frmt.get('rename')
                worksheet.write(0, col_index, value, header_format)

            # Define o autofilter # 0,0=A1 e 0,len = última célula da primeira linha
            worksheet.autofilter(0, 0, 0, len(df.columns) - 1)
            # Congela a primeira linha
            worksheet.freeze_panes(1, 0)
            writer.save()

            # Add timestamp and extension to the filename
            file_name = f"{file_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            response = HttpResponse(content.getvalue(), content_type='application/vnd.ms-excel')
            response['Content-Disposition'] = 'attachment; filename={}'.format(file_name)

            return response

    def prepare_to_export(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        target_columns = kwargs.get("target_columns", [])
        if target_columns:
            for column in target_columns:
                if column.get("name", "") in df.columns:
                    if column.get("type") == "decimal":
                        df = self.convert_to_float(
                            df, target_columns=[column["name"]], decimals=column.get("decimals", 2)
                        )
                    if column.get("type") == "integer":
                        df[column["name"]] = pd.to_numeric(df[column["name"]], errors="coerce").astype('Int64')
                else:
                    logging.warning(f"[prepare_to_export] {column.get('name', '')} doesn't exist!")

        for coluna in df.select_dtypes("category").columns:
            if df[coluna].isna().any():
                df[coluna] = df[coluna].astype("object")
        return df.replace({pd.NA: None})