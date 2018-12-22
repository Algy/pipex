import imgaug as ia

from imgaug import augmenters as iaa
from ..poperators import pipe

from typing import Optional

sometimes = lambda aug: iaa.Sometimes(0.5, aug)
augmenter_example = iaa.Sequential(
    [
        # apply the following augmenters to most images
        # iaa.Fliplr(0.5), # horizontally flip 50% of all images
        # iaa.Flipud(0.2), # vertically flip 20% of all images
        # crop images by -5% to 10% of their height/width
        sometimes(iaa.CropAndPad(
            percent=(-0.05, 0.1),
            pad_mode=ia.ALL,
            pad_cval=(0, 255)
        )),
        sometimes(iaa.Affine(
            scale={"x": (0.8, 1.2), "y": (0.8, 1.2)}, # scale images to 80-120% of their size, individually per axis
            translate_percent={"x": (-0.2, 0.2), "y": (-0.5, 0.5)}, # translate by -50 to +50 percent (per axis)
            # rotate=(-45, 45), # rotate by -45 to +45 degrees
            # shear=(-16, 16), # shear by -16 to +16 degrees
            order=[0, 1], # use nearest neighbour or bilinear interpolation (fast)
            cval=(0, 255), # if mode is constant, use a cval between 0 and 255
            mode=ia.ALL # use any of scikit-image's warping modes (see 2nd image from the top for examples)
        )),
        # execute 0 to 5 of the following (less important) augmenters per image
        # don't execute all of them, as that would often be way too strong
        iaa.SomeOf((0, 5),
            [
                iaa.OneOf([
                    iaa.GaussianBlur((0, 3.0)), # blur images with a sigma between 0 and 3.0
                    iaa.AverageBlur(k=(2, 7)), # blur image using local means with kernel sizes between 2 and 7
                    iaa.MedianBlur(k=(3, 11)), # blur image using local medians with kernel sizes between 2 and 7
                ]),
                iaa.Sharpen(alpha=(0, 1.0), lightness=(0.75, 1.5)), # sharpen images
                iaa.Emboss(alpha=(0, 1.0), strength=(0, 2.0)), # emboss images
                # search either for all edges or for directed edges,
                # blend the result with the original image using a blobby mask
                iaa.SimplexNoiseAlpha(iaa.OneOf([
                    iaa.EdgeDetect(alpha=(0.5, 1.0)),
                    iaa.DirectedEdgeDetect(alpha=(0.5, 1.0), direction=(0.0, 1.0)),
                ])),
                iaa.AdditiveGaussianNoise(loc=0, scale=(0.0, 0.05*255), per_channel=0.5), # add gaussian noise to images
                iaa.OneOf([
                    iaa.Dropout((0.01, 0.1), per_channel=0.5), # randomly remove up to 10% of the pixels
                    iaa.CoarseDropout((0.03, 0.15), size_percent=(0.02, 0.05), per_channel=0.2),
                ]),
                iaa.Invert(0.05, per_channel=True), # invert color channels
                iaa.Add((-10, 10), per_channel=0.5), # change brightness of images (by -10 to 10 of original value)
                iaa.AddToHueAndSaturation((-20, 20)), # change hue and saturation
                # either change the brightness of the whole image (sometimes
                # per channel) or change the brightness of subareas
                iaa.OneOf([
                    iaa.Multiply((0.5, 1.5), per_channel=0.5),
                    iaa.FrequencyNoiseAlpha(
                        exponent=(-4, 0),
                        first=iaa.Multiply((0.5, 1.5), per_channel=True),
                        second=iaa.ContrastNormalization((0.5, 2.0))
                    )
                ]),
                iaa.ContrastNormalization((0.5, 2.0), per_channel=0.5), # improve or worsen the contrast
                iaa.Grayscale(alpha=(0.0, 1.0)),
                sometimes(iaa.ElasticTransformation(alpha=(0.5, 3.5), sigma=0.25)), # move pixels locally around (with random strengths)
                sometimes(iaa.PiecewiseAffine(scale=(0.01, 0.05))), # sometimes move parts of the image around
                sometimes(iaa.PerspectiveTransform(scale=(0.01, 0.1)))
            ],
            random_order=True
        )
    ],
    random_order=True
)



class augment(pipe):
    def __init__(self, augmenter=augmenter_example, multiplier=20,
                 remove_out_of_image='partly'):
        self.augmenter = augmenter
        self.multiplier = multiplier
        self.remove_out_of_image = remove_out_of_image

    def _build_bounding_boxes(self, image, bboxes_xyxy) -> Optional[ia.BoundingBoxesOnImage]:
        if bboxes_xyxy is None:
            return None
        return ia.BoundingBoxesOnImage(
            [ia.BoundingBox(x, y, X, Y) for x, y, X, Y in bboxes_xyxy],
            image.shape,
        )

    def _cut_agd_box(self, agd_bboxes: Optional[ia.BoundingBoxesOnImage]):
        if agd_bboxes is None:
            return None
        remove_out_of_image = self.remove_out_of_image
        if remove_out_of_image == 'fully':
            agd_bboxes = agd_bboxes.remove_out_of_image(fully=True)
            agd_bboxes = agd_bboxes.cut_out_of_image()
        elif remove_out_of_image == 'partly':
            agd_bboxes = agd_bboxes.remove_out_of_image(partly=True)
            agd_bboxes = agd_bboxes.cut_out_of_image()
        return agd_bboxes

    def transform(self, our, precords):
        for precord in precords:
            multiplier = self.multiplier
            image_id = precord.id
            image = precord.value
            bboxes_xyxy = precord.get('bboxes_xyxy')
            ia_bbox = self._build_bounding_boxes(image, bboxes_xyxy)

            det = self.augmenter.to_deterministic()
            agd_arrs = det.augment_images([image] * multiplier)
            if ia_bbox is not None:
                agd_bboxes = det.augment_bounding_boxes([ia_bbox] * multiplier)
            else:
                agd_bboxes = [None] * multiplier

            for i, (agd_arr, agd_bbox) in enumerate(zip(agd_arrs, agd_bboxes)):
                new_id = "{image_id}_{i:03d}".format(image_id=image_id, i=i)
                agd_bbox = self._cut_agd_box(agd_bbox)
                channels = {"image_id": image_id}
                if agd_bbox is not None:
                    channels["bboxes_xyxy"] = agd_bbox.to_xyxy_array().tolist()
                    channels["image_augmented_mask"] = agd_bbox.draw_on_image(agd_arr)
                yield (
                    precord.with_channel_item("image_augmented", agd_arr).merge(**channels)
                    .with_id(new_id)
                )
