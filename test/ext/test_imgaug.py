import pytest
import numpy as np

from pipex import PRecord
from pipex.ext.imgaug import augment

@pytest.fixture
def image():
    return np.zeros((300, 300, 3), dtype=np.uint8)

def test_imgaug_simple(image):
    precord = PRecord.from_object(image, 'image', "my_image_id")

    results = list(enumerate([precord] >> augment))
    for i, aug_precord in results:
        assert set(aug_precord.channels) == set(["image", "image_augmented", "image_id"])
        assert aug_precord['image_id'] == 'my_image_id'
        assert aug_precord.id == "my_image_id_{:03d}".format(i)

        assert aug_precord.active_channel == 'image_augmented'
        assert (aug_precord['image'] == image).all()
        assert aug_precord.value_format == 'image'
    assert len(results) == 20

def test_imgaug_bboxes(image):
    precord = PRecord.from_object(image, 'image', "my_image_id")
    precord = precord.merge(bboxes_xyxy=[[0, 0, 10, 20], [100, 200, 150, 250]])

    results = list(enumerate([precord] >> augment(remove_out_of_image=None)))
    for i, aug_precord in results:
        assert set(aug_precord.channels) == set([
            "image", "image_augmented", 'image_augmented_mask',
            "image_id", "bboxes_xyxy"
        ])
        assert aug_precord.active_channel == 'image_augmented'

        assert isinstance(aug_precord['bboxes_xyxy'], list)
        assert len(aug_precord['bboxes_xyxy']) == 2
        assert len(aug_precord['bboxes_xyxy'][0]) == 4 # xyxy
        assert len(aug_precord['bboxes_xyxy'][1]) == 4 # xyxy
