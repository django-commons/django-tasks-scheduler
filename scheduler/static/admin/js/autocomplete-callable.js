// admin/js/autocomplete-callable.js
(function ($) {
    $(document).ready(function () {
        const inputField = $('.autocomplete-callable');

        inputField.autocomplete({
            source: function (request, response) {
                $.ajax({
                    url: '/scheduler/list-callables/', // URL to fetch suggestions
                    data: {
                        term: request.term // The current input value
                    },
                    success: function (data) {
                        response(data); // Pass the suggestions to the autocomplete widget
                    },
                    error: function () {
                        console.error('Error fetching autocomplete suggestions.');
                    }
                });
            },
            minLength: 2,
            select: function (event, ui) {
                inputField.val(ui.item.value); // Set the selected value
            }
        });
    });
})(django.jQuery);