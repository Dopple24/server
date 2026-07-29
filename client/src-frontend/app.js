const { invoke } = window.__TAURI__.core;

const output = document.getElementById("output");
const breadcrumb = document.getElementById("breadcrumb");
const entryList = document.getElementById("entry-list");
const upBtn = document.getElementById("up-btn");
const uploadBtn = document.getElementById("upload-btn");
const testBtn = document.getElementById("test-btn");

const username = sessionStorage.getItem("username");
const password = sessionStorage.getItem("password");

// Guard: if someone lands here without logging in (e.g. typed the URL,
// or refreshed after a restart that cleared sessionStorage), bounce back.
if (!username || !password) {
    window.location.href = "index.html";
}

// The full tree, fetched once. Navigation below just walks this in memory.
let rootFolder = null;

// Stack of folders from root to current, e.g. [root, sub, subsub].
// The last entry is always "where we are now".
let pathStack = [];

function showError(err) {
    output.textContent = "Error: " + (err?.toString?.() ?? String(err));
}

function clearError() {
    output.textContent = "";
}

function formatDate(iso) {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    });
}

function currentFolder() {
    return pathStack[pathStack.length - 1];
}

function currentFolderUuid() {
   return currentFolder().uuid;
}

function goToDepth(depth) {
    pathStack = pathStack.slice(0, depth + 1);
    render();
}

function openFolder(folder) {
    pathStack.push(folder);
    render();
}

function goUp() {
    if (pathStack.length > 1) {
        pathStack.pop();
        render();
    }
}

async function upload() {
  try {
    await invoke("upload", { username, password, folderUuid: currentFolderUuid() });
    fetchMap();
  }
  catch (err) {
      showError(err);
  }
}

async function test() {
  try {
    await invoke("test_dialog");
    fetchMap();
  }
  catch (err) {
      showError(err);
  }
}

function renderBreadcrumb() {
    breadcrumb.innerHTML = "";
    pathStack.forEach((folder, i) => {
        const crumb = document.createElement("span");
        crumb.className = "crumb";
        crumb.textContent = folder.name;
        if (i < pathStack.length - 1) {
            crumb.classList.add("crumb-link");
            crumb.addEventListener("click", () => goToDepth(i));
        } else {
            crumb.classList.add("crumb-current");
        }
        breadcrumb.appendChild(crumb);

        if (i < pathStack.length - 1) {
            const sep = document.createElement("span");
            sep.className = "crumb-sep";
            sep.textContent = "/";
            breadcrumb.appendChild(sep);
        }
    });

    upBtn.disabled = pathStack.length <= 1;
}

function buildFolderRow(folder) {
    const row = document.createElement("div");
    row.className = "entry-card folder-row";

    const title = document.createElement("div");
    title.className = "entry-title";
    title.textContent = `${folder.name}`;
    row.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "entry-meta";
    meta.textContent = `Updated ${formatDate(folder.last_changed_at)}`;
    row.appendChild(meta);

    row.addEventListener("click", () => openFolder(folder));
    return row;
}

function buildFileRow(file) {
    const row = document.createElement("div");
    row.className = "entry-card file-row";

    const title = document.createElement("div");
    title.className = "entry-title";
    title.textContent = `${file.name}`;
    row.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "entry-meta";
    meta.textContent = `Updated ${formatDate(file.last_changed_at)}`;
    row.appendChild(meta);

    return row;
}

function render() {
    const folder = currentFolder();
    renderBreadcrumb();
    entryList.innerHTML = "";

    const hasFolders = folder.folders && folder.folders.length > 0;
    const hasFiles = folder.files && folder.files.length > 0;

    if (!hasFolders && !hasFiles) {
        entryList.innerHTML = '<p class="empty">This folder is empty.</p>';
        return;
    }

    if (hasFolders) {
        folder.folders.forEach((f) => entryList.appendChild(buildFolderRow(f)));
    }
    if (hasFiles) {
        folder.files.forEach((f) => entryList.appendChild(buildFileRow(f)));
    }
}

async function fetchMap() {
    clearError();
    try {
      rootFolder = await invoke("request_map", { username, password });
      console.log(rootFolder);
        pathStack = [rootFolder];
        render();
    } catch (err) {
        showError(err);
    }
}

upBtn.addEventListener("click", goUp);
uploadBtn.addEventListener("click", upload);
testBtn.addEventListener("click", test);

document.getElementById("logout-btn").addEventListener("click", () => {
    sessionStorage.removeItem("username");
    sessionStorage.removeItem("password");
    window.location.href = "index.html";
});

document.getElementById("refresh-btn").addEventListener("click", fetchMap);

document.getElementById("run-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("run_task");
        output.textContent =
            typeof result === "string" ? result : JSON.stringify(result, null, 2);
    } catch (err) {
        showError(err);
    }
});

document.getElementById("greet-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("greet", { name: "World" });
        output.textContent = result;
    } catch (err) {
        showError(err);
    }
});

// Run automatically as soon as the page loads.
fetchMap();
