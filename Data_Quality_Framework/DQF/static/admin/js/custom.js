
var fileId = 0; // used by the addFile() function to keep track of IDs
function addFile(copyF, newE) {
    fileId++; // increment fileId to get a unique ID for the new element
    var html = document.getElementById(copyF).innerHTML;
    addElement('div', fileId, html, newE);
}
function addElement(elementTag, elementId, html, newE) {
    // Adds an element to the document
    var p = document.getElementById(newE);
    var newElement = document.createElement(elementTag);
    newElement.setAttribute('id', elementId);
    newElement.innerHTML = html;
    p.append(newElement);
}
function removeElement(btn) {
    // Removes an element from the document
    if (btn.parentNode.parentNode) {
        ((btn.parentNode).parentNode).parentNode.removeChild(btn.parentNode.parentNode);
    }
}

$(document).ready(function(){
    $('.attr_name').change(function() {
        let This=$(this);
        $.ajax({
            url: "add/req",
            data: {
                'attr_name' : This.val(),
            },
            success: function(data){
                This.parents('div.row_parent').find('select.attr_vals').html(data);
            },
            dataType: 'html',
        });

    });

    $('.row_parent').on('change','select.attr_name_new' , function() {
        let This=$(this);
        $.ajax({
            url: "add/req",
            data: {
                'attr_name' : This.val(),
            },
            success: function(data){
                This.parents('div.row_parent').find('select.attr_vals_new').html(data);
            },
            dataType: 'html',
        });

    });

    $('#blank_div2').on('change','select.attr_name_new' , function() {
        let This=$(this);
        $.ajax({
            url: "add/req",
            data: {
                'attr_name' : This.val(),
            },
            success: function(data){
                This.parents('div.row_parent').find('select.attr_vals_new').html(data);
            },
            dataType: 'html',
        });

    });

//    $('.attr_name').change(function() {
//        let This=$(this);
//        $.ajax({
//            url: "req",
//            data: {
//                'attr_name' : This.val(),
//            },
//            success: function(data){
//                This.parents('div.row_parent').find('select.attr_vals').html(data);
//            },
//            dataType: 'html',
//        });
//
//    });
//
//    $('.row_parent').on('change','select.attr_name_new' , function() {
//        let This=$(this);
//        $.ajax({
//            url: "req",
//            data: {
//                'attr_name' : This.val(),
//            },
//            success: function(data){
//                This.parents('div.row_parent').find('select.attr_vals_new').html(data);
//            },
//            dataType: 'html',
//        });
//
//    });
//
//    $('#blank_div2').on('change','select.attr_name_new' , function() {
//        let This=$(this);
//        $.ajax({
//            url: "req",
//            data: {
//                'attr_name' : This.val(),
//            },
//            success: function(data){
//                This.parents('div.row_parent').find('select.attr_vals_new').html(data);
//            },
//            dataType: 'html',
//        });
//
//    });

    //product atribute delete
    $('.delete_attr').on('click', function() {
        confirm('Are you sure?');
        let This=$(this);
        $.ajax({
            url: "del_attr",
            data: {
                'attr_id' : This.val(),
            },
            success: function(data){
                    This.closest('tr').remove()
            },
            dataType: 'html',
        });

    });

});



