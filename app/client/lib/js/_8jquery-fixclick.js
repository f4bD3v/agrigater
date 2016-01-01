/*
 * jQuery FixClick Plugin
 * version: 1.0
 * @requires jQuery v1.2.2 or later
 *
 * Copyright (c) 2008 AlloVince
 * Examples at: http://allo.ave7.net/fixclick
 * Licensed under the MIT License:
 *   http://www.opensource.org/licenses/mit-license.php
 *
 */
if (jQuery) {
    (function ($) {
        $.fn.fixClick = function (click_handler, dblclick_handler, immediate_click_handler) {
            var _ = this;
            _.click = click_handler;
            _.dblclick = dblclick_handler;
            _.click_immediate = immediate_click_handler;
            _.timer = null;
            $(this)
            .unbind("click.fixclick")
            .unbind("dblclick.fixclick")
            .on("click.fixclick", function (e) {
                var _self = this;
                _self.e = e;
                
                // Always invoke the 'immediate click' handler: that way we allow userland code to perform
                // some very specific tweaks that we cannot do for them (domain knowledge of the userland app required).
                if (immediate_click_handler) {                        
                    immediate_click_handler.call(_self, e);
                    // if the immediate_click_handler invoked e.stopImmediatePropagation() then we abort the remainder 
                    // of the activity here, including setting up the delayed firing of the click event itself.
                    if (e.isImmediatePropagationStopped()) {
                        return;
                    }
                }

                if (!_.timer) {
                    // As we delay the click event we need to (shallow) clone the event as we must prevent
                    // any manipulations of the event object from reaching our registered click event handler.
                    // This way we prevent the e.preventDefault(), etc. below from killing any rigorously
                    // checking event handler invoked in here:
                    var originalEvent = e.originalEvent;
                    var event;
                    if (originalEvent) {
                        if (document.createEvent) {
                            event = document.createEvent("MouseEvents");
                            event.initMouseEvent(
                                originalEvent.type, 
                                originalEvent.bubbles, 
                                originalEvent.cancelable, 
                                originalEvent.view, 
                                originalEvent.detail,
                                originalEvent.screenX, 
                                originalEvent.screenY, 
                                originalEvent.clientX, 
                                originalEvent.clientY,
                                originalEvent.ctrlKey, 
                                originalEvent.altKey, 
                                originalEvent.shiftKey, 
                                originalEvent.metaKey,
                                originalEvent.button, 
                                originalEvent.relatedTarget || document.body.parentNode 
                            );
                        } 
                        else if ( document.createEventObject ) {
                            event = this.event();
                            event.button = event.button;
                        }
                        if (event) {
                            for (var attr in originalEvent) {
                                if (originalEvent.hasOwnProperty(attr) && typeof originalEvent[attr] !== 'function') {
                                    event[attr] = originalEvent[attr];
                                }
                            }
                        }
                    }

                    var e2 = $.extend(
                        new $.Event(),
                        e,
                        {
                            isSimulated: true,
                            originalEvent: event
                        }
                    );
                    if (e.isDefaultPrevented()) {
                        e2.preventDefault();
                    }
                    if (e.isPropagationStopped()) {
                        e2.stopPropagation();
                    }

                    _.timer = setTimeout(function () {
                        click_handler.call(_self, e2);
                        _.timer = null;
                    }, $.fn.fixClick.clickDelay);
                }
                // Since we delay the click event to prevent it from firing as part of a doubleclick event,
                // we have to assume a few things.
                // 
                // Here we assume we DO NOT want this event to 'bubble up' the DOM tree as we'll be
                // handling it here, anyway.
                e.stopPropagation();
            })
            .on("dblclick.fixclick", function (e) {
                var _self = this;
                clearTimeout(_.timer);
                _.timer = null;
                dblclick_handler.call(_self, e);
            });
            return this;
        };

        $.fn.fixClick.clickDelay = 300;
    })(jQuery);
}
