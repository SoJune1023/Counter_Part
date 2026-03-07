import { tableFromIPC } from "https://cdn.jsdelivr.net/npm/apache-arrow@15.0.0/+esm";
let gridApi;

document.addEventListener('DOMContentLoaded', () => {
    const fileInput = document.getElementById('fileInput');
    const accountNameInput = document.getElementById('accountNameInput');
    const uploadBtn = document.getElementById('uploadBtn');
    const uploadedDownloadBtn = document.getElementById('uploadedDownloadBtn')
    const analyzeBtn = document.getElementById('analyzeBtn');
    const analyzeDownloadBtn = document.getElementById('analyzeDownloadBtn');
    const statusDiv = document.getElementById('statusText');
    const fileNameSpan = document.getElementById('fileName');

    const numberFormatter = (params) => {
        if (params.value == null) { return ''; }
        
        try {
            return Number(params.value).toLocaleString('ko-KR');
        } catch (e) {
            return params.value;
        }
    };
    
    function updateButtonStates() {
        const hasFile = fileInput.files.length > 0;
        const accountValue = accountNameInput.value.trim();
        const hasAccount = accountValue.length > 0 && accountValue !== "계정과목";
        
        uploadBtn.disabled = !hasFile;
        uploadedDownloadBtn.disabled = !hasFile;
        
        analyzeBtn.disabled = !(hasFile && hasAccount);
        analyzeDownloadBtn.disabled = !(hasFile && hasAccount);
        
        if (hasFile) {
            fileNameSpan.innerText = fileInput.files[0].name;
        }
    };
    
    fileInput.addEventListener('change', updateButtonStates);
    
    accountNameInput.addEventListener('input', updateButtonStates);
    
    uploadBtn.onclick = async () => {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);

        try {
            uploadBtn.disabled = true;
            statusDiv.innerText = "파일 분석 및 스트리밍 중 . . .";

            const response = await fetch('/analyze/file', {
                method: 'POST',
                body: formData
            });
            
            if (!response.ok) throw new Error('서버 처리 실패');

            
            const table = await tableFromIPC(response);

            const columnDefs = table.schema.fields.map(f => {
                const colDef = {
                    field: f.name,
                    headerName: f.name,
                    sortable: true,
                    filter: true,
                    resizable: true,
                    minWidth: 100,
                    suppressSizeToFit: true
                };

                if (f.name.includes('차변') || f.name.includes('대변') || f.name.includes('금액')) {
                    colDef.valueFormatter = numberFormatter;
                    colDef.cellStyle = { textAlign: 'right' };
                }

                return colDef;
            });

            const rowData = [];
            const rawData = table.toArray();
            for (let i = 0; i < rawData.length; i++) {
                rowData.push(rawData[i].toJSON());
            }

            const gridOptions = {
                columnDefs: columnDefs,
                rowData: rowData,
                pagination: false, 
                rowBuffer: 100,
                debounceVerticalScrollbar: true,
                suppressColumnVirtualisation: false,
                alwaysShowVerticalScroll: true,

                autoSizeStrategy: {
                    type: 'fitCellContents',
                    skipHeader: false
                },

                onFirstDataRendered: (params) => {
                    setTimeout(() => {
                        const allColumnIds = params.api.getAllGridColumns().map(col => col.getColId());
                        params.api.autoSizeColumns(allColumnIds, false);
                    }, 0);
                }
            };

            const gridDiv = document.querySelector('#grid-table');
            gridDiv.innerHTML = "";
            gridApi = agGrid.createGrid(gridDiv, gridOptions);

            setTimeout(() => {
                gridApi.sizeColumnsToFit();
            }, 100);

            statusDiv.innerText = `파일 불러오기 완료: ${table.numRows.toLocaleString()} 행 표시 중`;
        } catch (error) {
            statusDiv.innerText = "에러: " + error.message;
            console.error(error);
        } finally {
            uploadBtn.disabled = false;
        }
    };

    uploadedDownloadBtn.onclick = async () => {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);
        formData.append('account_name', accountNameInput.value.trim());

        try {
            uploadedDownloadBtn.disabled = true;
            statusDiv.innerText = "데이터 다운로드 중 . . .";

            const response = await fetch('/analyze/file/download', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) throw new Error('서버 처리 실패');

            const blob = await response.blob();
            const excelBlob = new Blob([blob], { 
                type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' 
            });

            const url = window.URL.createObjectURL(excelBlob);
            const a = document.createElement('a');
            a.href = url;

            a.download = `simple_${fileInput.files[0].name}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);

            statusDiv.innerText = "파일 변환 완료";
        } catch (error) {
            statusDiv.innerText = "에러: " + error.message;
            console.error(error);
        } finally {
            uploadedDownloadBtn.disabled = false;
        }
    };

    analyzeBtn.onclick = async () => {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);
        formData.append('account_name', accountNameInput.value.trim());

        try {
            analyzeBtn.disabled = true;
            statusDiv.innerText = "데이터 분석 및 스트리밍 중 . . .";
            
            const response = await fetch('/analyze/account_table', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) throw new Error('서버 처리 실패');

            const table = await tableFromIPC(response);

            const columnDefs = table.schema.fields.map(f => {
                const colDef = {
                    field: f.name,
                    headerName: f.name,
                    sortable: true,
                    filter: true,
                    resizable: true,
                    minWidth: 100,
                    suppressSizeToFit: true
                };

                if (f.name.includes('차변') || f.name.includes('대변') || f.name.includes('금액')) {
                    colDef.valueFormatter = numberFormatter;
                    colDef.cellStyle = { textAlign: 'right' };
                }

                return colDef;
            });
            
            const rowData = [];
            const rawData = table.toArray();
            for (let i = 0; i < rawData.length; i++) {
                rowData.push(rawData[i].toJSON());
            }

            const gridOptions = {
                columnDefs: columnDefs,
                rowData: rowData,
                pagination: false, 
                rowBuffer: 100,
                debounceVerticalScrollbar: true,
                suppressColumnVirtualisation: false,
                alwaysShowVerticalScroll: true,

                autoSizeStrategy: {
                    type: 'fitCellContents',
                    skipHeader: false
                },

                onFirstDataRendered: (params) => {
                    setTimeout(() => {
                        const allColumnIds = params.api.getAllGridColumns().map(col => col.getColId());
                        params.api.autoSizeColumns(allColumnIds, false);
                    }, 0);
                }
            };

            const gridDiv = document.querySelector('#result-grid-table');
            gridDiv.innerHTML = "";
            gridApi = agGrid.createGrid(gridDiv, gridOptions);

            setTimeout(() => {
                gridApi.sizeColumnsToFit();
            }, 100);

            statusDiv.innerText = `파일 분석 완료: ${table.numRows.toLocaleString()} 행 표시 중`;
        } catch (error) {
            statusDiv.innerText = "에러: " + error.message;
            console.error(error);
        } finally {
            analyzeBtn.disabled = false;
        }
    };

    analyzeDownloadBtn.onclick = async () => {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);
        formData.append('account_name', accountNameInput.value.trim());

        try {
            analyzeDownloadBtn.disabled = true;
            statusDiv.innerText = "데이터 다운로드 중 . . .";

            const response = await fetch('/analyze/account_table/download', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) throw new Error('서버 처리 실패');

            const blob = await response.blob();
            const excelBlob = new Blob([blob], { 
                type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' 
            });

            const url = window.URL.createObjectURL(excelBlob);
            const a = document.createElement('a');
            a.href = url;

            a.download = `pivoted_${fileInput.files[0].name}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);

            statusDiv.innerText = "파일 변환 완료";
        } catch (error) {
            statusDiv.innerText = "에러: " + error.message;
            console.error(error);
        } finally {
            analyzeDownloadBtn.disabled = false;
        }
    };
})