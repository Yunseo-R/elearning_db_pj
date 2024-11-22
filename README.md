# Excel to MariaDB Data Import Script
## 소개
이 프로젝트는 엑셀 파일에서 데이터를 읽어와 MariaDB 데이터베이스에 저장하는 파이썬 스크립트입니다.<br>
엑셀 파일을 읽고, 정보를 추출해 데이터베이스에 삽입합니다.<br> 
데이터베이스 형식 및 생성 쿼리는 **241114 전남e학습터 검색시스템 테이블정의서-v0.4**에 명시되어있습니다. <br>

## 요구사항
**Python 버전**: 이 스크립트는 Python 3.13 버전을 사용하여 작성되었습니다. 사용하시려면 Python을 설치해주세요.
- **필요 라이브러리**: mariadb, pandas, openpyxl의 세 가지 라이브러리를 설치해야 합니다.
- 아래 명령어를 터미널에 입력하여 라이브러리를 설치해주세요.
  ```bash
  pip install mariadb pandas openpyxl
  ```

## 사용법
1. **환경 설정**: MariaDB 데이터베이스에 접근할 수 있는 설정이 필요합니다.
   - `connect()` 함수는 데이터베이스에 접속하기 위한 정보를 담고 있습니다. 사용자 정보와 DB정보, 포트 주소 등을 정의합니다.
   - 사용자 정보와 DB명은 테이블정의서 내용을 기본값으로 하며, host와 port는 로컬환경을 기준으로 설정되었습니다. 참고하여 적절하게 수정해주세요. 

2. **엑셀 파일 경로 설정**:
   - `main()` 함수 내부에 있는 `excel_file_path` 변수에 엑셀 파일의 경로를 지정해야 합니다.
   - 엑셀 파일의 경로를 지정하려면 파일을 **우클릭**한 후 **경로로 복사**를 선택한 후 변수에 지정하세요.
   - 예시: `C:\Users\사용자명\Desktop\test.xlsx`<br><br>
   **참고**
   - 만약 읽기 에러 발생한다면, 변수에 지정한 경로의 모든 \에 \를 하나씩 더 추가하세요.
   - 예시: `C:\\Users\\사용자명\\Desktop\\test.xlsx`

3. **스크립트 실행**:
   - 아래 명령어로 스크립트를 실행하세요.   
   ```bash
   python main.py
   ```

4. **데이터 삽입 과정**:
   - `main()`에서 `excel_file_path` 변수에 명시된 경로에서 엑셀 파일을 불러와 데이터를 읽어들입니다.
   - 시트이름에서 초/중 여부를 판단해 추출하고, `extract_year_semester()` 함수로 첫번째 셀에서 연도와 학기를 추출하는 등 데이터를 처리합니다.
   - 엑셀 파일에서 읽고 처리가 끝난 데이터는 `insert_data()` 함수를 통해 데이터베이스에 삽입됩니다.


## 주의사항 
- 이 스크립트는 **업로드 파일(샘플) 폴더**에 있는 엑셀 파일 형식을 기준으로 작성되었습니다. 엑셀 파일의 표 형식에 변형이 있을 시 스크립트에 오류가 발생할 수 있습니다.
- 엑셀 파일의 첫 번째 행은 제목으로 간주합니다. `extract_year_semester()` 함수는 엑셀 파일의 첫 번째 셀에서 연도와 학기를 추출합니다. 해당 셀에 유효한 정보가 없을 경우 ValueError가 발생합니다.
- 실제 데이터는 엑셀 파일의 5행 이후부터 시작되는 것으로 명시되어 있습니다. 
- 엑셀 파일의 시트명에 따라 '초'는 ele_school에 '초', '중'은 mid_school에 '중'으로 삽입됩니다.


## 오류 처리
- **엑셀 파일 경로 오류**: 엑셀 파일 경로가 잘못되었거나 파일이 없을 경우, `FileNotFoundError`가 발생하며 오류 메시지를 출력합니다.
- **연도/학기 추출 실패**: 엑셀 파일의 첫 번째 셀에서 연도와 학기를 추출할 수 없으면 `ValueError`가 발생합니다.

