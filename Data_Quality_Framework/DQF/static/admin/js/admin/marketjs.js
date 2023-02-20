
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
            url: "req",
            data: {
                'attr_name' : This.val(),
            },
            success: function(data){
                This.parents('div.row_parent').find('select.attr_vals').html(data);
            },
            dataType: 'html',
        }); 
        
    });


    $('#blank_div2').on('change','select.attr_name_new' , function() {
        let This=$(this);
        $.ajax({           
            url: "req",
            data: {
                'attr_name' : This.val(),
            },
            success: function(data){
                This.parents('div.row_parent').find('select.attr_vals_new').html(data);
            },
            dataType: 'html',
        }); 
        
    });
    


    $('.row_parent').on('change','select.attr_name_new' , function() {
        let This=$(this);
        $.ajax({           
            url: "req",
            data: {
                'attr_name' : This.val(),
            },
            success: function(data){
                This.parents('div.row_parent').find('select.attr_vals_new').html(data);
            },
            dataType: 'html',
        }); 
        
    });



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




//product image delete
    $('.delete_img').on('click', function() {
        confirm('Are you sure?');        
        let This=$(this);
        $.ajax({           
            url: "del_img",
            data: {
                'img_id' : This.val(),
            },
            success: function(data){
                    This.closest('tr').remove()
            },
            dataType: 'html',
        }); 
        
    });

//attribute value delete
    $('.del_val').click(function() {
        confirm('Are you sure?');
        let This=$(this);
        $.ajax({           
            url: "attr_val_del",
            data: {
                'img_id' : This.val(),
            },
            success: function(data){
                    This.closest('tr').remove()
            },
            dataType: 'html',
        }); 
        
    });


    










    //CSV ajax calls
    $('#coupon_csv').click(function() {
        let This=$(this);
        let value = This.parents('.box-body').find('#coupon_search');
        document.location.href = 'coupons/csv?coupon_filter=' + value.val();
       
    });




    $('#customer_csv').click(function() {
        let This=$(this);
        let value_from = This.parents('.box-body').find('#from').val();
        let value_to = This.parents('.box-body').find('#to').val();
        document.location.href = 'customers/csv?from=' + value_from +'&to=' + value_to;
    });



    $('#sales_csv').click(function() {
        let This=$(this);
        let value = This.parents('.box-body').find('#to_filter').val();
        console.log(value);
        document.location.href = 'sales/csv?to_filter=' + value;
       
    });






});



