const { invoke } = window.__TAURI__.core;

const loginError = document.getElementById("login-error");

document.getElementById("go-to-register").addEventListener("click", () => {
    window.location.href = "register.html";
});

document.getElementById("login-btn").addEventListener("click", async () => {
    const username = document.getElementById("login-username").value;
    const password = document.getElementById("login-password").value;
    loginError.textContent = "";

    try {
        const success = await invoke("try_login", { username, password });
        if (success) {
            // Stored only for this window's session, cleared on full app restart.
            sessionStorage.setItem("username", username);
            sessionStorage.setItem("password", password);
            window.location.href = "app.html";
        } else {
            loginError.textContent = "Invalid username or password.";
        }
    } catch (err) {
        loginError.textContent = "Error: " + (err?.toString?.() ?? String(err));
    }
});
