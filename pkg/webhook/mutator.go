package webhook

import (
	"context"
	"encoding/json"
	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// Mutator defines interface for a mutation webHook
type Mutator interface {
	// Prototype returns a prototype of Object for this admission request.
	Prototype(req admission.Request) (runtime.Object, error)

	// MutateCreate handles Object creation and returns the object after mutation and error if any.
	MutateCreate(ctx context.Context, obj runtime.Object) (runtime.Object, error)
	// MutateUpdate handles Object update and returns the object after mutation and error if any.
	MutateUpdate(ctx context.Context, obj runtime.Object, oldObj runtime.Object) (runtime.Object, error)
}

// NewMutatingWebhook creates a new mutating Webhook.
func NewMutatingWebhook(mutator Mutator) *admission.Webhook {
	return &admission.Webhook{
		Handler: &mutatingHandler{mutator: mutator},
	}
}

var _ admission.DecoderInjector = &mutatingHandler{}
var _ admission.Handler = &mutatingHandler{}

type mutatingHandler struct {
	mutator Mutator
	decoder *admission.Decoder
}

// InjectDecoder injects the decoder into a mutatingHandler.
func (h *mutatingHandler) InjectDecoder(d *admission.Decoder) error {
	h.decoder = d
	return nil
}

// Handle handles admission requests.
func (h *mutatingHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Operation {
	case admissionv1beta1.Create:
		return h.handleCreate(ctx, req)
	case admissionv1beta1.Update:
		return h.handleUpdate(ctx, req)
	default:
		return admission.Allowed("")
	}
}

func (h *mutatingHandler) handleCreate(ctx context.Context, req admission.Request) admission.Response {
	prototype, err := h.mutator.Prototype(req)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	obj := prototype.DeepCopyObject()
	if err := h.decoder.DecodeRaw(req.Object, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	mutatedObj, err := h.mutator.MutateCreate(ctx, obj)
	if err != nil {
		return admission.Denied(err.Error())
	}
	mutatedObjPayload, err := json.Marshal(mutatedObj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, mutatedObjPayload)
}

func (h *mutatingHandler) handleUpdate(ctx context.Context, req admission.Request) admission.Response {
	prototype, err := h.mutator.Prototype(req)
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

	mutatedObj, err := h.mutator.MutateUpdate(ctx, obj, oldObj)
	if err != nil {
		return admission.Denied(err.Error())
	}
	mutatedObjPayload, err := json.Marshal(mutatedObj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, mutatedObjPayload)
}
