# pykrx Daily Download Diagnostic

Requested date: 2026-06-16

## Environment

- python_version: 3.11.9 (tags/v3.11.9:de54cf5, Apr  2 2024, 10:12:12) [MSC v.1938 64 bit (AMD64)]
- python_executable: C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System\.venv\Scripts\python.exe
- platform: Windows-10-10.0.26200-SP0
- pykrx_version: 1.2.8
- pandas_version: 2.3.3
- requests_version: 2.34.2
- current_working_directory: C:\Users\Dell3571\Desktop\PROJECTS\LLM_mini_PJT\AI_Trading_System
- system_time: 2026-06-17T14:05:24

## Network Sanity Check

- success: True
- status_code: 200
- content_type: text/html;charset=UTF-8
- elapsed_seconds: 0.27
- first_200_chars_sanitized: <html lang="ko">  <head>    <meta charset="utf-8">    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">    <title>정보데이터시스템</title>    <script type="text/javascript" src="/WEB-APP/web
- exception: : 

## Endpoint Matrix

### stock.get_market_ticker_list KOSPI / 20240614

- elapsed_seconds: 0.25
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 20240614

- elapsed_seconds: 0.25
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 20240614

- elapsed_seconds: 0.27
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 20240614

- elapsed_seconds: 0.26
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 20240614

- elapsed_seconds: 0.05
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '79700', '고가': '80500', '저가': '79000', '종가': '79600', '거래량': '22926612', '등락률': '1.2722646310432568'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 2024-06-14

- elapsed_seconds: 0.24
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 2024-06-14

- elapsed_seconds: 0.27
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 2024-06-14

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 2024-06-14

- elapsed_seconds: 0.29
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 2024-06-14

- elapsed_seconds: 0.04
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '79700', '고가': '80500', '저가': '79000', '종가': '79600', '거래량': '22926612', '등락률': '1.2722646310432568'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 20250613

- elapsed_seconds: 0.25
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 20250613

- elapsed_seconds: 0.28
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 20250613

- elapsed_seconds: 0.26
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 20250613

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 20250613

- elapsed_seconds: 0.03
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '60200', '고가': '60200', '저가': '57700', '종가': '58300', '거래량': '20705979', '등락률': '-2.0168067226890756'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 2025-06-13

- elapsed_seconds: 0.27
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 2025-06-13

- elapsed_seconds: 0.24
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 2025-06-13

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 2025-06-13

- elapsed_seconds: 0.33
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 2025-06-13

- elapsed_seconds: 0.03
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '60200', '고가': '60200', '저가': '57700', '종가': '58300', '거래량': '20705979', '등락률': '-2.0168067226890756'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 20260612

- elapsed_seconds: 0.23
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 20260612

- elapsed_seconds: 0.25
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 20260612

- elapsed_seconds: 0.38
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 20260612

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 20260612

- elapsed_seconds: 0.02
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '326000', '고가': '339000', '저가': '320000', '종가': '322500', '거래량': '31006148', '등락률': '7.859531772575251'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 2026-06-12

- elapsed_seconds: 0.24
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 2026-06-12

- elapsed_seconds: 0.27
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 2026-06-12

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 2026-06-12

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 2026-06-12

- elapsed_seconds: 0.02
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '326000', '고가': '339000', '저가': '320000', '종가': '322500', '거래량': '31006148', '등락률': '7.859531772575251'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 20260616

- elapsed_seconds: 0.22
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 20260616

- elapsed_seconds: 0.24
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 20260616

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 20260616

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 20260616

- elapsed_seconds: 0.02
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '343000', '고가': '345500', '저가': '332500', '종가': '343000', '거래량': '17548685', '등락률': '1.7804154302670623'}]
- exception: : 

### stock.get_market_ticker_list KOSPI / 2026-06-16

- elapsed_seconds: 0.24
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ticker_list KOSDAQ / 2026-06-16

- elapsed_seconds: 0.37
- success: False
- type: list
- shape_or_row_count: (0,)
- columns: []
- first_3_rows: []
- exception: : endpoint returned empty; likely market data unavailable for date

### stock.get_market_ohlcv_by_ticker KOSPI / 2026-06-16

- elapsed_seconds: 0.26
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_ticker KOSDAQ / 2026-06-16

- elapsed_seconds: 0.24
- success: False
- type: 
- shape_or_row_count: 0
- columns: []
- first_3_rows: []
- exception: KeyError: "None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"

### stock.get_market_ohlcv_by_date 005930 / 2026-06-16

- elapsed_seconds: 0.03
- success: True
- type: DataFrame
- shape_or_row_count: (1, 6)
- columns: ['시가', '고가', '저가', '종가', '거래량', '등락률']
- first_3_rows: [{'시가': '343000', '고가': '345500', '저가': '332500', '종가': '343000', '거래량': '17548685', '등락률': '1.7804154302670623'}]
- exception: : 

## Summary

- total_checks: 40
- failed_checks: 32
- likely_market_data_unavailable_or_endpoint_issue: True

