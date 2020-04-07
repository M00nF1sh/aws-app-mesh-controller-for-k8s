package webhook

import (
	"context"
	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// Validator defines interface for a validation webHook
type Validator interface {
	// Prototype returns a prototype of Object for this admission request.
	Prototype(req admission.Request) (runtime.Object, error)

	// ValidateCreate handles Object creation and returns error if any.
	ValidateCreate(ctx context.Context, obj runtime.Object) error
	// ValidateUpdate handles Object update and returns error if any.
	ValidateUpdate(ctx context.Context, obj runtime.Object, oldObj runtime.Object) error
	// ValidateDelete handles Object deletion and returns error if any.
	ValidateDelete(ctx context.Context, obj runtime.Object) error
}

// NewValidatingWebhook creates a new validating Webhook.
func NewValidatingWebhook(validator Validator) *admission.Webhook {
	return &admission.Webhook{
		Handler: &validatingHandler{validator: validator},
	}
}

var _ admission.DecoderInjector = &validatingHandler{}
var _ admission.Handler = &validatingHandler{}

type validatingHandler struct {
	validator Validator
	decoder   *admission.Decoder
}

// InjectDecoder injects the decoder into a mutatingHandler.
func (h *validatingHandler) InjectDecoder(d *admission.Decoder) error {
	h.decoder = d
	return nil
}

// Handle handles admission requests.
func (h *validatingHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Operation {
	case admissionv1beta1.Create:
		return h.handleCreate(ctx, req)
	case admissionv1beta1.Update:
		return h.handleUpdate(ctx, req)
	case admissionv1beta1.Delete:
		return h.handleDelete(ctx, req)
	default:
		return admission.Allowed("")
	}
}

func (h *validatingHandler) handleCreate(ctx context.Context, req admission.Request) admission.Response {
	prototype, err := h.validator.Prototype(req)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	obj := prototype.DeepCopyObject()
	if err := h.decoder.DecodeRaw(req.Object, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := h.validator.ValidateCreate(ctx, obj); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

func (h *validatingHandler) handleUpdate(ctx context.Context, req admission.Request) admission.Response {
	prototype, err := h.validator.Prototype(req)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	obj := prototype.DeepCopyObject()
	oldObj := prototype.DeepCopyObject()
	if err := h.decoder.DecodeRaw(req.Object, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := h.decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := h.validator.ValidateUpdate(ctx, obj, oldObj); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

func (h *validatingHandler) handleDelete(ctx context.Context, req admission.Request) admission.Response {
	prototype, err := h.validator.Prototype(req)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	obj := prototype.DeepCopyObject()
	if err := h.decoder.DecodeRaw(req.OldObject, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := h.validator.ValidateDelete(ctx, obj); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}
