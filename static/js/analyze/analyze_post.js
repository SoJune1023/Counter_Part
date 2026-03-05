import { tableFromIPC } from "https://cdn.jsdelivr.net/npm/apache-arrow@15.0.0/+esm";

document.addEventListener('DOMContentLoaded', () => {
    const fileInput = document.getElementById('fileInput');
    const uploadBtn = document.getElementById('uploadBtn');
    const statusDiv = document.getElementById('statusText');
    const fileNameSpan = document.getElementById('fileName');
    
    let dataGrid = null;

    fileInput.addEventListener('change', () => {
        if (fileInput.files.length > 0) {
            fileNameSpan.innerText = fileInput.files[0].name;
            uploadBtn.disabled = false;
        }
    });

    uploadBtn.onclick = async () => {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);

        try {
            uploadBtn.disabled = true;
            statusDiv.innerText = "⏳ 서버 분석 및 스트리밍 중...";

            const response = await fetch('/analyze', { method: 'POST', body: formData });
            if (!response.ok) throw new Error('서버 처리 실패');

            // 1. Arrow Stream 파싱
            const table = await tableFromIPC(response);

            // 3. 표 데이터 추출 (Grid.js용)
            const columnNames = table.schema.fields.map(f => f.name);
            const gridData = [];
            for (let i = 0; i < Math.min(table.numRows, 10000); i++) { // 최대 1만건 표시
                gridData.push(columnNames.map((_, j) => table.getChildAt(j).get(i)));
            }
            renderTable(columnNames, gridData);
            
            statusDiv.innerText = `✅ 완료: ${table.numRows.toLocaleString()} 로우 처리됨`;
        } catch (error) {
            statusDiv.innerText = "❌ 에러: " + error.message;
            console.error(error);
        } finally {
            uploadBtn.disabled = false;
        }
    };

    function renderTable(columns, data) {
        const container = document.getElementById("grid-table");
        container.innerHTML = "";
        dataGrid = new gridjs.Grid({
            columns: columns,
            data: data,
            sort: true,
            search: true,
            resizable: true,
            // height: '400px'
        }).render(container);
    }
});