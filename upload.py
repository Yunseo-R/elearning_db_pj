import mariadb
import pandas as pd
from datetime import datetime
from db_connector import DBConnector


class DataUploader(DBConnector):
    """
    엑셀 파일의 데이터를 읽어 데이터베이스에 업로드하고 업로드 이력을 관리하는 클래스.
    DBConnector를 상속받아 DB연결을 관리
    """

    def __init__(self):
        """DataUploader 클래스 초기화"""
        super().__init__()

    def validate_file(self, file_path: str) -> bool:
        """
        주어진 파일 경로와 확장자를 검증하는 메서드.

        Args:
            file_path (str): 업로드할 엑셀 파일의 경로.

        Returns:
            bool: 파일이 유효한 경우 True 반환.

        Raises:
            ValueError: 파일 확장자가 .xlsx 또는 .xls가 아닌 경우 예외 발생.
        """
        if not file_path.endswith(('.xlsx', '.xls')):
            raise ValueError("파일 확장자가 유효하지 않습니다. (.xlsx 또는 .xls 파일만 지원)")
        return True

    def parse_and_upload(self, file_path: str, year: int, semester: int):
        """
        엑셀 파일을 파싱하여 데이터베이스에 업로드하는 메서드.
        업로드 과정 중 이력 관리를 포함하며, 업로드 성공 여부에 따라 상태를 갱신합니다.

        Args:
            file_path (str): 업로드할 엑셀 파일의 경로.
            year (int): 해당 데이터의 연도 (드롭다운에서 선택된 값).
            semester (int): 해당 데이터의 학기 (드롭다운에서 선택된 값).

        Raises:
            ValueError: 업로드 과정에서 오류 발생시 오류 종류 로그
        """
        status = 0  # 초기 상태: 처리중
        try:
            # 업로드 이력 저장 (처리중 상태)
            self.save_upload_log(year, semester, file_path, status)

            # 엑셀 파일 읽기
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')

            # 각 시트를 반복 처리
            for sheet_name in excel_file.sheet_names:
                try:
                    # 학교 구분(school_type) 설정 : (sheet_name에 따라 '초' 또는 '중' 설정)
                    if sheet_name in ['초', '초등', '초등학교']:
                        school_type = '초'
                    elif sheet_name in ['중', '중등', '중학교']:
                        school_type = '중'
                    else:
                        raise ValueError(f"파일 형식 오류: 시트명 '{sheet_name}'의 형식이 올바르지 않습니다.")

                    # 5번째 행부터 데이터를 읽어오고, 필요한 열 이름 설정
                    data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', skiprows=4)
                    required_columns = ['연번', '청번', '청명', '소속', '성명', '나이스 개인번호',
                                        '생년월일', '영역', '시작일', '종료일', '비고']
                    data.columns = required_columns
                except ValueError as ve:
                    raise ValueError(f"파일 형식 오류: {ve}")
                except Exception as e:
                    raise ValueError(f"시트 '{sheet_name}'를 읽는 중 오류 발생: {e}")

                # 데이터베이스 연결 및 데이터 삽입
                conn = self.connect()
                try:
                    # 데이터 삽입 호출
                    self._insert_data(conn, data, year, semester, school_type)
                    print(f"'{sheet_name}' 시트의 데이터가 성공적으로 업로드되었습니다.")
                except mariadb.Error as e:
                    conn.rollback()  # 삽입 오류 발생 시 롤백
                    print(f"데이터 삽입 중 오류 발생: {e}")
                    raise
                finally:
                    conn.close()

            # 업로드 완료 상태로 업로드 이력 갱신
            status = 1
            self.update_upload_status(file_path, status)
        except Exception as e:
            # 업로드 실패 상태로 업로드 이력 갱신
            status = 2
            self.update_upload_status(file_path, status)
            raise ValueError(f"업로드 실패: {e}")

    def _insert_data(self, conn, data: pd.DataFrame, year: int, semester: int, school_type: str):
        """
        DataFrame 데이터를 DB에 삽입하는 내부 메서드.

        Args:
            conn: 데이터베이스 연결 객체.
            data (pd.DataFrame): 업로드할 데이터.
            year (int): 해당 데이터의 연도.
            semester (int): 해당 데이터의 학기.
            school_type (str): 학교 유형 ('초' 또는 '중').

        Raises:
            mariadb.Error: 데이터 삽입 중 오류 발생 시 예외 발생.
        """
        cursor = conn.cursor()
        query = ("INSERT INTO TB_ELEARNING_DATA (year, semester, agency_name, affiliation, "
                 "agency_number, school_type, t_name, nice_number, birthday, "
                 "area, start_date, end_date, reg_date, remarks) "
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

        for _, row in data.iterrows():
            # 소속과 성명이 모두 비어있는 행은 건너뛰기
            if pd.isna(row['소속']) and pd.isna(row['성명']):
                continue

            # 청번을 정수로 변환, 실패 시 None 처리
            try:
                agency_number = int(row['청번']) if not pd.isna(row['청번']) else None
            except ValueError:
                agency_number = None

            try:
                # 데이터 삽입
                cursor.execute(query, (
                    year, semester,
                    row.get('청명', None),
                    row.get('소속', None),
                    agency_number,
                    school_type,
                    row.get('성명', None),
                    row.get('나이스 개인번호', None),
                    row.get('생년월일', None),
                    row.get('영역', None),
                    int(row['시작일']) if not pd.isna(row['시작일']) else None,
                    int(row['종료일']) if not pd.isna(row['종료일']) else None,
                    datetime.now(),
                    row.get('비고', None) if not pd.isna(row.get('비고', None)) else None
                ))
            except Exception as e:
                conn.rollback()  # 데이터 삽입 오류 발생 시 롤백
                raise mariadb.Error(f"데이터 삽입 실패: {e}")

        conn.commit()  # 모든 데이터 삽입 성공 시 커밋

    def save_upload_log(self, year: int, semester: int, file_name: str, status: int):
        """
        업로드 이력을 tb_upload_data에 저장하는 메서드.

        Args:
            year (int): 선택연도.
            semester (int): 선택학기.
            file_name (str): 업로드된 파일 이름.
            status (int): 업로드 상태 (0: 처리중, 1: 완료, 2: 실패).
        Raises:
            mariadb.Error: 업로드 이력 저장 중 오류 발생 시 예외 발생.
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            reg_date = datetime.now()
            cursor.execute("""
                INSERT INTO TB_UPLOAD_DATA (year, semester, reg_date, file_name, status)
                VALUES (?, ?, ?, ?, ?)
            """, (year, semester, reg_date, file_name, status))
            conn.commit()
        except mariadb.Error as e:
            if 'conn' in locals():
                conn.rollback()
            raise mariadb.Error(f"업로드 이력 저장 중 오류 발생: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

    def update_upload_status(self, file_name: str, status: int):
        """
        업로드 상태 업데이트 메서드.

        Args:
            file_name (str): 업로드된 파일 이름.
            status (int): 업데이트할 업로드 상태 (0: 처리중, 1: 완료, 2: 실패).
        Raises:
            mariadb.Error: 상태 업데이트 중 오류 발생 시 예외 발생.
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE TB_UPLOAD_DATA
                SET status = ?
                WHERE file_name = ?
            """, (status, file_name))
            conn.commit()
        except mariadb.Error as e:
            if 'conn' in locals():
                conn.rollback()
            raise mariadb.Error(f"업로드 상태 업데이트 중 오류 발생: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

    def fetch_upload_logs(self):
        """
        업로드 이력 조회 메서드

        Returns:
            List[Dict[str, Any]]: 업로드 이력 목록. 각 이력은 딕셔너리로 반환.
                - dat_idx (int): 데이터 인덱스.
                - year (int): 연도.
                - semester (int): 학기.
                - reg_date (datetime): 등록일.
                - file_name (str): 파일 이름.
                - status (str): 상태 (처리중/업로드 완료/업로드 실패).
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()

            # 업로드 이력 조회 쿼리
            cursor.execute("""
                SELECT dat_idx, year, semester, reg_date, file_name, status
                FROM TB_UPLOAD_DATA
                ORDER BY reg_date DESC
            """)
            columns = [desc[0] for desc in cursor.description]

            # 상태 값 변환
            results = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                if record['status'] == 0:
                    record['status'] = "처리중"
                elif record['status'] == 1:
                    record['status'] = "업로드 완료"
                elif record['status'] == 2:
                    record['status'] = "업로드 실패"
                results.append(record)

            conn.close()
            return results
        except mariadb.Error as e:
            print(f"업로드 이력 조회 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return []
