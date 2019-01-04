var showDataDirForm = function(){
    var form = document.getElementById('data_dir_form');
    var show_form_btn = document.getElementById('show_form_button');
    var input_field = document.getElementById('data_dir_input_field');
    form.className = '';
    input_field.focus();
    show_form_btn.style.display = 'none';
};