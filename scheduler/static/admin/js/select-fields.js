(function ($) {
    $(function () {
        const tasktypes = {
            "CronTask": $(".tasktype-CronTask"),
            "RepeatableTask": $(".tasktype-RepeatableTask"),
            "OnceTask": $(".tasktype-OnceTask"),
        };
        var taskTypeField = $('#id_task_type');

        function toggleVerified(value) {
            console.log(value);
            for (const [k, v] of Object.entries(tasktypes)) {
                if (k === value) {
                    v.show();
                } else {
                    v.hide();
                }
            }
        }

        toggleVerified(taskTypeField.val());

        taskTypeField.change(function () {
            toggleVerified($(this).val());
        });
    });
})(django.jQuery);