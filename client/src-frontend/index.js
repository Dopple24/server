const { invoke } = window.__TAURI__.core;

const loginView = document.getElementById("login-view");
const appView = document.getElementById("app-view");
const loginError = document.getElementById("login-error");
const output = document.getElementById("output");

// Kept in memory only, so request_map can reuse the credentials
// without asking the user to type them again.
let credentials = { username: "", password: "" };

function showOutput(value) {
    if (typeof value === "string") {
        output.textContent = value;
    } else {
        output.textContent = JSON.stringify(value, null, 2);
    }
}

function showError(err) {
    output.textContent = "Error: " + (err?.toString?.() ?? String(err));
}

function showApp() {
    loginView.classList.add("hidden");
    appView.classList.remove("hidden");
}

function showLogin() {
    appView.classList.add("hidden");
    loginView.classList.remove("hidden");
    loginError.textContent = "";
    output.textContent = "";
}

document.getElementById("login-btn").addEventListener("click", async () => {
    const username = document.getElementById("login-username").value;
    const password = document.getElementById("login-password").value;
    loginError.textContent = "";

    try {
        const success = await invoke("try_login", { username, password });
        if (success) {
            credentials = { username, password };
            showApp();
        } else {
            loginError.textContent = "Invalid username or password.";
        }
    } catch (err) {
        loginError.textContent = "Error: " + (err?.toString?.() ?? String(err));
    }
});

document.getElementById("logout-btn").addEventListener("click", () => {
    credentials = { username: "", password: "" };
    document.getElementById("login-username").value = "";
    document.getElementById("login-password").value = "";
    showLogin();
});

document.getElementById("run-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("run_task");
        showOutput(result);
    } catch (err) {
        showError(err);
    }
});

document.getElementById("greet-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("greet", { name: "World" });
        showOutput(result);
    } catch (err) {
        showError(err);
    }
});

document.getElementById("map-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("request_map", {
            username: credentials.username,
            password: credentials.password,
        });
        showOutput(result);
    } catch (err) {
        showError(err);
    }
});
