document.addEventListener('DOMContentLoaded', () => {
    const fileInput = document.getElementById('fileInput');
    const accountInput = document.getElementById('accountNameInput');
    const uploadBtn = document.getElementById('uploadBtn');
    const fileNameSpan = document.getElementById('fileName');
    const statusDiv = document.getElementById('status');

    // 1. 버튼 상태 제어 함수
    const checkFormValidity = () => {
        const hasFile = fileInput.files.length > 0;
        const isNotDefault = accountInput.value.trim() !== "계정과목";
        const hasAccount = accountInput.value.trim().length > 0;
        
        console.log(`파일 있음: ${hasFile}, 계정과목 입력됨: ${hasAccount}`); // 디버깅용
        
        // 둘 다 만족해야 버튼 활성화 (disabled 해제)
        uploadBtn.disabled = !(hasFile && hasAccount && isNotDefault);
    };

    // 2. 파일 선택 이벤트
    fileInput.addEventListener('change', () => {
        if (fileInput.files.length > 0) {
            fileNameSpan.innerText = fileInput.files[0].name;
            console.log("파일 선택됨:", fileInput.files[0].name);
        }
        checkFormValidity();
    });

    // 3. 계정과목 입력 이벤트
    accountInput.addEventListener('input', () => {
        checkFormValidity();
    });

    // 4. 업로드 실행
    uploadBtn.onclick = async () => {
        const file = fileInput.files[0];
        const accountName = accountInput.value.trim();

        const formData = new FormData();
        formData.append('file', file);
        formData.append('account_name', accountName);

        try {
            uploadBtn.disabled = true;
            statusDiv.innerText = "⏳ 서버 처리 중...";
            
            const response = await fetch('/analyze/account_table', {
                method: 'POST',
                body: formData
            });

            console.log("서버 응답 수신:", response.status);

            if (!response.ok) throw new Error('서버 처리 실패');

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `pivoted_${file.name}`;
            a.click();
            
            statusDiv.innerText = "✅ 완료되었습니다!";
        } catch (error) {
            statusDiv.innerText = "❌ 에러: " + error.message;
        } finally {
            checkFormValidity(); // 작업 완료 후 다시 상태 체크
        }
    };

    // 페이지 로드 시 초기 체크
    checkFormValidity();
});