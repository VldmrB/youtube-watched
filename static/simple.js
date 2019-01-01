var showDataDirForm = function(){
    var hidden_elem = document.getElementById('change_form');
    var show_form_btn = document.getElementById('show_form_button');
    var input = document.getElementById('data_dir_input');
    if (hidden_elem.style.display == 'none') {
        hidden_elem.style.display = 'block';
        input.focus();
        show_form_btn.innerHTML = 'Cancel';
    } else {
        hidden_elem.style.display = 'none';
        show_form_btn.style.display = 'inline';
        show_form_btn.innerHTML = 'Change...';
    }
};