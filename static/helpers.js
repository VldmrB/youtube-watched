function toggleElementVisibility(togglingElementId, elementToToggle,
                                 togglingElementText = "Cancel", mainInputId = null) {
    let button = document.querySelector("#" + togglingElementId);
    let buttonText = button.innerHTML;
    let buttonWidth = button.offsetWidth;
    let formElement = document.querySelector("#" + elementToToggle);
    button.addEventListener("click", function() {
        if (button.innerHTML === buttonText) {
            formElement.classList.toggle("hidden");
            button.style.width = buttonWidth.toString() + 'px';
            button.innerHTML = togglingElementText;
            if (mainInputId) {
                setTimeout( function(){document.querySelector("#" + mainInputId).focus();}, 1);
            }
        } else {
            button.innerHTML = buttonText;
            formElement.classList.toggle("hidden");
        }
    });
}