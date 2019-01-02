var showDataDirForm = function(){
    var form = document.getElementById('change_form');
    var show_form_btn = document.getElementById('show_form_button');
    var input_field = document.getElementById('data_dir_input_field');
    if (form.style.display == 'none') {
        form.style.display = 'block';
        input_field.focus();
        show_form_btn.innerHTML = 'Cancel';
    } else {
        form.style.display = 'none';
        show_form_btn.style.display = 'inline';
        show_form_btn.innerHTML = 'Change...';
    }
};