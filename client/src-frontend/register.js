const { invoke } = window.__TAURI__.core;

const registerError = document.getElementById("register-error");
const registerSuccess = document.getElementById("register-success");
const registerBtn = document.getElementById("register-btn");

document.getElementById("register-btn").addEventListener("click", async () => {
    const username = document.getElementById("register-username").value;
    const password = document.getElementById("register-password").value;
    const adminPass = document.getElementById("register-admin-pass").value;
    registerError.textContent = "";
    registerSuccess.textContent = "";

    if (!username || !password || !adminPass) {
        registerError.textContent = "All fields are required.";
        return;
    }

    registerBtn.disabled = true;

    try {
        await invoke("register", {
            username,
            password,
            adminPass,
        });
        registerSuccess.textContent = `Account "${username}" created! Redirecting to login…`;
        setTimeout(() => {
            window.location.href = "index.html";
        }, 1500);
    } catch (err) {
        registerError.textContent = "Error: " + (err?.toString?.() ?? String(err));
        registerBtn.disabled = false;
    }
});

document.getElementById("back-to-login").addEventListener("click", () => {
    window.location.href = "index.html";
});
