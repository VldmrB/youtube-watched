function toggleElementVisibilityAndOtherButton() {
    let newProjectButton = document.querySelector("#setup-project-button");
    let newProjectButtonWidth = newProjectButton.offsetWidth;
    let newProjectButtonText = "New...";
    let newProjectFormDiv = document.querySelector("#setup-project-container");
    
    let switchProjectButton = document.querySelector("#switch-project-button");
    let switchProjectButtonWidth = switchProjectButton.offsetWidth;
    let switchProjectButtonText = "Switch to...";
    let switchProjectFormDiv = document.querySelector("#switch-project-container");

    function bindAction(button, buttonWidth, buttonText, buttonFormDiv,
                        otherButton, otherButtonText, otherButtonFormDiv) {
        if (buttonFormDiv.classList.contains("hidden")) {
            buttonFormDiv.classList.toggle("hidden");
            button.innerHTML = "Cancel";
            button.style.width = buttonWidth + "px";
            if (!otherButton.classList.contains("hidden")) {
                otherButtonFormDiv.classList.add("hidden");
                otherButton.innerHTML = otherButtonText;
            }
        } else {
            buttonFormDiv.classList.toggle("hidden");
            button.innerHTML = buttonText;
        }
    }
    newProjectButton.addEventListener("click", function() {
        bindAction(newProjectButton, newProjectButtonWidth, newProjectButtonText, newProjectFormDiv,
            switchProjectButton, switchProjectButtonText, switchProjectFormDiv);
    });
    switchProjectButton.addEventListener("click", function() {
        bindAction(switchProjectButton, switchProjectButtonWidth, switchProjectButtonText, switchProjectFormDiv,
            newProjectButton, newProjectButtonText, newProjectFormDiv);
    });
}

toggleElementVisibilityAndOtherButton();
