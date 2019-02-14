if (!document.querySelector("#takeout-section").dataset.full) {
document.querySelector("#takeout-setup").classList.remove("hidden");
} else {
    toggleElementVisibility("takeout-setup-button", "takeout-setup", "Cancel", "takeout-input");
}

document.querySelector("#new-project-button").onclick = function () {
    window.location = "/setup_project";
};

let progressMsg = document.querySelector("#progress-msg");
let progressBar = document.querySelector("#progress-bar");
let progressBarPercentage = document.querySelector("#progress-bar-text span");
let progressUnfinishedDBProcessWarning = document.querySelector("#wait-for-db");
let takeoutSubmit = document.querySelector("#takeout-form");
let takeoutSubmitButton = takeoutSubmit.querySelector("input[type='submit']");
let updateRecordsButton = document.querySelector("#update-records-button");
let takeoutCancelButton = document.querySelector("#takeout-cancel-button");

function cleanUpProgressBar() {
    document.querySelector("#progress-bar-container").style.display = "none";
    takeoutCancelButton.style.display = "none";
    progressBar.style.width = "0%";
    progressBarPercentage.innerHTML = "0%";
    progressMsg.innerHTML = "";
    progressMsg.style.color = "black";
}

function processTakeout(event) {
    event.preventDefault();
    let idOfElementActedOn = this.id;
    let takeoutDirectoryVal = document.querySelector("#takeout-input").value;
    let progress = new EventSource("/db_progress_stream");

    function closeEventSource() {
        progress.close();
    }

    function cleanUpAfterTakeoutInsertion() {
        takeoutSubmitButton.removeAttribute("disabled");
        updateRecordsButton.removeAttribute("disabled");
        takeoutCancelButton.style.display = "none";
        try {
            closeEventSource();
        } catch(err) {}
        window.removeEventListener("beforeunload", closeEventSource);
    }

    function makeCancelButtonCancel() {
        takeoutCancelButton.setAttribute("disabled", "true");
        closeEventSource();
        progressMsg.innerHTML = "Stopping the process, please wait...";
        let cancelAJAX = new XMLHttpRequest();
        cancelAJAX.open("POST", "cancel_db_process");
        cancelAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
        function cancelTakeout() {
            if (cancelAJAX.readyState === 4 && cancelAJAX.status === 200) {
                cleanUpAfterTakeoutInsertion();
                cleanUpProgressBar();
                takeoutCancelButton.removeAttribute("disabled");
                takeoutCancelButton.removeEventListener("click", makeCancelButtonCancel);
            }
        }
        cancelAJAX.addEventListener("readystatechange", cancelTakeout);
        cancelAJAX.send();
    }

    function showProgress() {
        if (anAJAX.readyState === 4 && anAJAX.status === 200) {
            if (anAJAX.responseText.indexOf("Wait for") !== -1) {
                if (progressUnfinishedDBProcessWarning.innerHTML === "") {
                    progressUnfinishedDBProcessWarning.innerHTML = anAJAX.responseText;
                    setTimeout(function() {progressUnfinishedDBProcessWarning.innerHTML = "";}, 3000);
                    closeEventSource();
                }
            } else {
                cleanUpProgressBar();
                takeoutCancelButton.style.display = "inline-block";
                document.querySelector("#progress-bar-container").style.display = "flex";
                takeoutSubmitButton.setAttribute("disabled", "true");
                updateRecordsButton.setAttribute("disabled", "true");

                window.addEventListener("beforeunload", closeEventSource);
                progress.onmessage = function (event) {
                    if (event.data.length < 6) {
                        let progressVal = event.data + "%";
                        progressBar.style.width = progressVal;
                        progressBarPercentage.innerHTML = progressVal;
                    } else {
                        progressMsg.innerHTML = event.data;
                        if (event.data.indexOf("records_processed") !== -1) {
                            progressBar.style.width = "100%";
                            progressBarPercentage.innerHTML = "100%";
                            let msgJSON = JSON.parse(event.data);
                            let msgString = "Records processed: " + msgJSON["records_processed"];
                            if (msgJSON.hasOwnProperty("records_inserted")) {
                                msgString += "<br>Inserted: " + msgJSON["records_inserted"];
                            }
                            msgString += "<br>Updated: " + msgJSON["records_updated"];

                            if (msgJSON.hasOwnProperty("records_in_db")) {
                                msgString += "<br>Total in the database: " + msgJSON["records_in_db"];
                            }
                            if (msgJSON.hasOwnProperty("failed_api_requests") &&
                                msgJSON["failed_api_requests"] !== 0) {
                                msgString += ("<br>Failed API requests: " + msgJSON["failed_api_requests"] +
                                    " (run this again to attempt these 1-2 more times.)");
                            }
                            if (msgJSON.hasOwnProperty("dead_records") && msgJSON["dead_records"] !== 0) {
                                msgString += ("<br>Videos with no identifying info: " + msgJSON["dead_records"] +
                                    " (added as unknown)");
                            }
                            progressMsg.innerHTML = msgString;
                            cleanUpAfterTakeoutInsertion();
                            takeoutCancelButton.removeEventListener("click", makeCancelButtonCancel);

                        } else if (event.data.indexOf("Error") !== -1) {
                            progressMsg.style.color = "red";
                            cleanUpAfterTakeoutInsertion();
                            takeoutCancelButton.removeEventListener("click", makeCancelButtonCancel);
                        }
                    }
                };
            }
        }
    }

    takeoutCancelButton.addEventListener("click", makeCancelButtonCancel);

    let anAJAX = new XMLHttpRequest();
    if (idOfElementActedOn === "takeout-form") {
        anAJAX.open("POST", "/convert_takeout");
        anAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
        anAJAX.addEventListener("readystatechange", showProgress);
        anAJAX.send("takeout-dir=" + takeoutDirectoryVal);
    } else {
        anAJAX.open("GET", "/update_records");
        anAJAX.addEventListener("readystatechange", showProgress);
        anAJAX.send();
    }

}

takeoutSubmit.addEventListener("submit", processTakeout);
updateRecordsButton.addEventListener("click", processTakeout);