const { invoke } = window.__TAURI__.core;

const svg_icon = `<svg class=\"file-icon\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"><path d=\"M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z\" /><path d=\"M14 2v6h6\" /></svg>`;

const output = document.getElementById("output");
const breadcrumb = document.getElementById("breadcrumb");
const entryList = document.getElementById("entry-list");
const upBtn = document.getElementById("up-btn");
const uploadBtn = document.getElementById("upload-btn");
const newFolderBtn = document.getElementById("new-folder-btn");

const transferManager = document.getElementById("transfer-manager");
const transferColumn = document.getElementById("transfer-column");
const transferHeader = transferManager.querySelector(".transfer-header");

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
    const transferId = crypto.randomUUID();
    addTransferRow(transferId);

    try {
      let file_name = await invoke("upload", { username, password, folderUuid: currentFolderUuid() });
        updateFileName(transferId, file_name)
      updateTransferProgress(transferId, 100);
    } catch (err) {
      showError(err);
    } finally {
      removeTransferRow(transferId);
      fetchMap();
      loadPendingTransfers();
    }
}

async function uploadReinit(uuid) {
  try {
    let file_name = await invoke("upload_reinit", { username, password, sendUuid: uuid });
    updateTransferProgress(uuid, 100);
  }
  catch (err) {
      showError(err);
  } finally {
      removeTransferRow(uuid);
    fetchMap();
    loadPendingTransfers();
  }
}

async function download(uuid) {
    const transferId = crypto.randomUUID();
    addTransferRow(transferId);
    runDownload(transferId, uuid, false);
}

async function runDownload(transferId, uuid, isRetry) {
  try {
    if (isRetry) {
      await invoke("download_reinit", { username, password, accUuid: uuid });
    } else {
      await invoke("download", { username, password, fileUuid: uuid });
    }
    setTransferSuccess(transferId);
  } catch (err) {
    const message = err?.toString?.() ?? String(err);
    setTransferError(transferId, message, () => runDownload(transferId, uuid, true));
  } finally {
    removeTransferRow(transferId);
    loadPendingTransfers();
  }
}

async function download_reinit(uuid) {
    const transferId = crypto.randomUUID();
    addTransferRow(transferId);

    try {
        // if your Rust command reports progress via events, listen and call:
        // updateTransferProgress(transferId, percent);
        await invoke("download_reinit", { username, password, accUuid: uuid });
        updateTransferProgress(transferId, 100);
    } catch (err) {
        showError(err);
    } finally {
      removeTransferRow(transferId);
      loadPendingTransfers();
    }
}

async function newFolder(folderName) {
  try {
    await invoke("create_folder", { username, password, folderUuid: currentFolderUuid(), folderName });
    fetchMap();
    loadPendingTransfers();
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

  const delete_folder_btn = document.createElement("button");
  delete_folder_btn.className = "delete-btn";
  delete_folder_btn.textContent = "Delete";
  delete_folder_btn.addEventListener("click", (e) => {
    e.stopPropagation();
    deleteFolder(folder.uuid);
  });

  row.appendChild(delete_folder_btn);

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

  const delete_btn = document.createElement("button");
  delete_btn.className = "delete-btn";
  delete_btn.textContent = `Delete`;
  delete_btn.addEventListener("click", () => deleteFile(file.uuid))
  row.appendChild(delete_btn);

  const download_btn = document.createElement("button");
  download_btn.className = "delete-btn";
  download_btn.textContent = `Download`;
  download_btn.addEventListener("click", () => download(file.uuid))
  row.appendChild(download_btn);

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
    const savedPath = currentPathUuids();

    try {
        rootFolder = await invoke("request_map", { username, password });
        pathStack = findPathByUuids(rootFolder, savedPath);
        render();
    } catch (err) {
        showError(err);
    }
}

upBtn.addEventListener("click", goUp);
uploadBtn.addEventListener("click", upload);

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

function updateTransferHeader() {
    const count = transferColumn.children.length;
    transferHeader.textContent = `Transferring ${count} file${count === 1 ? "" : "s"}`;
    transferManager.style.display = count === 0 ? "none" : "";
}

function addTransferRow(id, filename) {
    const row = document.createElement("div");
    row.className = "transfer-row";
    row.dataset.transferId = id;

    const item = document.createElement("div");
    item.className = "transfer-item";

    const nameSpan = document.createElement("span");
    nameSpan.className = "file-name";
    nameSpan.innerHTML =
        `${svg_icon}
        ${filename ?? id}`
    ;
    item.appendChild(nameSpan);

    const percentSpan = document.createElement("span");
    percentSpan.className = "file-percent";
    percentSpan.textContent = "0%";
    item.appendChild(percentSpan);

    row.appendChild(item);

    const track = document.createElement("div");
    track.className = "transfer-progress-track";
    const bar = document.createElement("div");
    bar.className = "transfer-progress-bar";
    bar.style.width = "0%";
    track.appendChild(bar);
    row.appendChild(track);

    transferColumn.appendChild(row);
    updateTransferHeader();
    return row;
}

async function deleteFile(uuid) {
  try {
    await invoke("delete_file", { username, password, uuid });
    fetchMap();
  }
  catch (err) {
    showError(err)
  }
}

async function deleteFolder(uuid) {
  try {
    await invoke("delete_folder", { username, password, uuid });
    fetchMap();
  }
  catch (err) {
    showError(err)
  }
}

function updateTransferProgress(id, percent) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (!row) return;
    row.querySelector(".file-percent").textContent = `${percent}%`;
    row.querySelector(".transfer-progress-bar").style.width = `${percent}%`;
}

function updateFileName(id, file_name) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (!row) return;
    row.querySelector(".file-name").innerHTML = `${svg_icon}${file_name}`;
}

function removeTransferRow(id) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (row) row.remove();
    updateTransferHeader();
}

function setTransferError(id, message, onRetry) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (!row) return;

    row.classList.add("transfer-error");
    row.querySelector(".file-percent").textContent = "Failed";

    let errorLine = row.querySelector(".transfer-error-msg");
    if (!errorLine) {
        errorLine = document.createElement("div");
        errorLine.className = "transfer-error-msg";
        row.appendChild(errorLine);
    }
    errorLine.textContent = message;

    let retryBtn = row.querySelector(".retry-btn");
    if (!retryBtn) {
        retryBtn = document.createElement("button");
        retryBtn.className = "retry-btn";
        retryBtn.textContent = "Retry";
        row.appendChild(retryBtn);
    }
    retryBtn.onclick = () => {
        row.classList.remove("transfer-error");
        errorLine.remove();
        retryBtn.remove();
        row.querySelector(".file-percent").textContent = "0%";
        onRetry();
    };
}

function setTransferSuccess(id) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (!row) return;
    updateTransferProgress(id, 100);
    row.classList.add("transfer-success");
    setTimeout(() => removeTransferRow(id), 1000);
}

async function loadPendingTransfers() {
    try {
      const parts = await invoke("request_parts");
      console.log(parts);

        parts.acc.forEach((entry) => {
            const transferId = crypto.randomUUID();
            const filename = entry.real_path.split("/").pop();
            addTransferRow(transferId, filename);
            setTransferError(
                transferId,
                "Incomplete — resume to continue",
                () => runDownload(transferId, entry.uuid, true)
            );
        });

        parts.send.forEach((entry) => {
          const filename = entry.filename;
          console.log(entry);
            addTransferRow(entry.uuid, filename);
            setTransferError(
                entry.uuid,
                "Incomplete — resume to continue",
                () => uploadReinit(entry.uuid, true) // once upload_reinit exists
            );
        });
    } catch (err) {
        showError(err);
    }
}

function startNewFolderRow() {
    if (entryList.querySelector(".new-folder-row")) return;

    const emptyMsg = entryList.querySelector(".empty");
    if (emptyMsg) emptyMsg.remove();

    const row = document.createElement("div");
    row.className = "entry-card new-folder-row";

    const input = document.createElement("input");
    input.type = "text";
    input.className = "new-folder-input";
    input.value = "New Folder";
    row.appendChild(input);

    const errorLine = document.createElement("div");
    errorLine.className = "new-folder-error";
    row.appendChild(errorLine);

    entryList.prepend(row);
    input.focus();
    input.select();

    let isSubmitting = false;

    function cancel() {
        row.remove();
    }

    async function confirm() {
        if (isSubmitting) return;

        const name = input.value.trim();
        if (!name) {
            cancel();
            return;
        }

        isSubmitting = true;
        input.disabled = true;
        errorLine.textContent = "";

        try {
            await invoke("create_folder", {
                username,
                password,
                folderUuid: currentFolderUuid(),
                folderName: name,
            });
            row.remove();
            fetchMap();
            loadPendingTransfers();
        } catch (err) {
            errorLine.textContent = err?.toString?.() ?? String(err);
            input.disabled = false;
            isSubmitting = false;
            input.focus();
            input.select();
        }
    }

    input.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            confirm();
        } else if (e.key === "Escape") {
            cancel();
        }
    });

    input.addEventListener("blur", () => {
        if (!isSubmitting) confirm();
    });
}

newFolderBtn.addEventListener("click", startNewFolderRow);

function currentPathUuids() {
    // Skip the root itself (index 0), since root has no "which child" info needed
    return pathStack.slice(1).map((f) => f.uuid);
}

function findPathByUuids(root, uuids) {
    const path = [root];
    let current = root;

    for (const uuid of uuids) {
        const next = (current.folders || []).find((f) => f.uuid === uuid);
        if (!next) break; // folder no longer exists (deleted, moved, etc.) — stop here
        path.push(next);
        current = next;
    }

    return path;
}

// Run automatically as soon as the page loads.
fetchMap();
loadPendingTransfers();
